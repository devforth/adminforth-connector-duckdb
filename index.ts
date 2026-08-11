import {
  DuckDBConnection,
  DuckDBInstance,
  type DuckDBValue,
} from '@duckdb/node-api';
import dayjs from 'dayjs';
import {
  AdminForthBaseConnector,
  AdminForthDataTypes,
  AdminForthFilterOperators,
  AdminForthSortDirections,
  type AdminForthConfig,
  type AdminForthResource,
  type AdminForthResourceColumn,
  type IAdminForthAndOrFilter,
  type IAdminForthDataSourceConnector,
  type IAdminForthSingleFilter,
  type IAdminForthSort,
  type IAggregationRule,
  type IGroupByDateTrunc,
  type IGroupByField,
  type IGroupByRule,
  checkIfFieldIsInsideResourceColumns,
  dbLogger,
} from 'adminforth';

type DuckDBRow = Record<string, any>;

const ARRAY_TYPE_RE = /^(.+)\[(\d*)\]$/;
const DECIMAL_TYPE_RE = /^(?:DECIMAL|NUMERIC)\((\d+),(\d+)\)$/;
const DUCKDB_URL_PREFIX_RE = /^duckdb:\/\//;
const TIMESTAMP_TIMEZONE_SUFFIX_RE = /(?:Z|[+-]\d{2}(?::?\d{2})?)$/i;
const INTEGER_TYPES = new Set([
  'TINYINT', 'SMALLINT', 'INTEGER', 'BIGINT', 'HUGEINT',
  'UTINYINT', 'USMALLINT', 'UINTEGER', 'UBIGINT',
]);
const FLOAT_TYPES = new Set(['FLOAT', 'REAL', 'DOUBLE', 'DOUBLE PRECISION']);
const STRING_TYPES = new Set(['VARCHAR', 'CHAR', 'BPCHAR', 'STRING', 'UUID', 'ENUM']);
const TIMESTAMP_TYPES = new Set([
  'TIMESTAMP', 'TIMESTAMP_S', 'TIMESTAMP_MS', 
  'TIMESTAMP_NS', 'TIMESTAMP WITH TIME ZONE', 'TIMESTAMPTZ'
]);
const TIME_TYPES = new Set(['TIME', 'TIME WITH TIME ZONE', 'TIMETZ']);

function quoteIdentifier(identifier: string): string {
  return identifier
    .split('.')
    .map((part) => `"${part.replaceAll('"', '""')}"`)
    .join('.');
}

function tableParts(table: string): { schema: string; name: string } {
  const parts = table.split('.');
  return parts.length === 1
    ? { schema: 'main', name: parts[0] }
    : { schema: parts.at(-2) as string, name: parts.at(-1) as string };
}

function normalizeBoundValue(value: any): DuckDBValue {
  if (value instanceof Date) {
    return value.toISOString();
  }
  if (Array.isArray(value) || (value !== null && typeof value === 'object')) {
    return JSON.stringify(value);
  }
  if (typeof value === 'bigint') {
    return value.toString();
  }
  return value;
}

function typeMetadata(dataType: string): Partial<AdminForthResourceColumn> & Record<string, any> {
  const normalizedType = dataType.toUpperCase();
  const arrayMatch = normalizedType.match(ARRAY_TYPE_RE);

  if (arrayMatch) {
    const itemMetadata = typeMetadata(arrayMatch[1]);
    if (itemMetadata.type === AdminForthDataTypes.JSON) {
      return {
        type: AdminForthDataTypes.JSON,
        _underlineType: 'array',
        _arrayItemUnderlineType: itemMetadata._underlineType,
      };
    }
    return {
      type: AdminForthDataTypes.JSON,
      isArray: {
        enabled: true,
        itemType: itemMetadata.type ?? AdminForthDataTypes.STRING,
      },
      _underlineType: 'array',
      _arrayItemUnderlineType: itemMetadata._underlineType,
    };
  }

  if (INTEGER_TYPES.has(normalizedType)) {
    return { type: AdminForthDataTypes.INTEGER, _underlineType: normalizedType.toLowerCase() };
  }
  if (FLOAT_TYPES.has(normalizedType)) {
    return { type: AdminForthDataTypes.FLOAT, _underlineType: normalizedType.toLowerCase() };
  }

  const decimalMatch = normalizedType.match(DECIMAL_TYPE_RE);
  if (decimalMatch || normalizedType === 'DECIMAL' || normalizedType === 'NUMERIC') {
    return {
      type: AdminForthDataTypes.DECIMAL,
      _underlineType: 'decimal',
      precision: decimalMatch ? Number(decimalMatch[1]) : undefined,
      scale: decimalMatch ? Number(decimalMatch[2]) : undefined,
    };
  }
  if (normalizedType === 'BOOLEAN' || normalizedType === 'BOOL') {
    return { type: AdminForthDataTypes.BOOLEAN, _underlineType: 'boolean' };
  }
  if (normalizedType === 'DATE') {
    return { type: AdminForthDataTypes.DATE, _underlineType: 'date' };
  }
  if (TIMESTAMP_TYPES.has(normalizedType)) {
    return { type: AdminForthDataTypes.DATETIME, _underlineType: normalizedType.toLowerCase() };
  }
  if (TIME_TYPES.has(normalizedType)) {
    return { type: AdminForthDataTypes.TIME, _underlineType: normalizedType.toLowerCase() };
  }
  if (normalizedType === 'JSON' || normalizedType.startsWith('STRUCT(') || normalizedType.startsWith('MAP(') || normalizedType.startsWith('UNION(')) {
    return { type: AdminForthDataTypes.JSON, _underlineType: normalizedType.toLowerCase() };
  }
  if (STRING_TYPES.has(normalizedType) || normalizedType.startsWith('ENUM(')) {
    return { type: AdminForthDataTypes.STRING, _underlineType: normalizedType.toLowerCase() };
  }
  if (normalizedType === 'BLOB' || normalizedType === 'BIT' || normalizedType === 'INTERVAL') {
    return { type: AdminForthDataTypes.TEXT, _underlineType: normalizedType.toLowerCase() };
  }
  return { type: AdminForthDataTypes.STRING, _underlineType: normalizedType.toLowerCase() };
}

class DuckDBConnector extends AdminForthBaseConnector implements IAdminForthDataSourceConnector {
  private instance?: DuckDBInstance;

  declare client: DuckDBConnection;

  OperatorsMap = {
    [AdminForthFilterOperators.EQ]: '=',
    [AdminForthFilterOperators.NE]: 'IS DISTINCT FROM',
    [AdminForthFilterOperators.GT]: '>',
    [AdminForthFilterOperators.LT]: '<',
    [AdminForthFilterOperators.GTE]: '>=',
    [AdminForthFilterOperators.LTE]: '<=',
    [AdminForthFilterOperators.LIKE]: 'LIKE',
    [AdminForthFilterOperators.ILIKE]: 'ILIKE',
    [AdminForthFilterOperators.IN]: 'IN',
    [AdminForthFilterOperators.NIN]: 'NOT IN',
    [AdminForthFilterOperators.AND]: 'AND',
    [AdminForthFilterOperators.OR]: 'OR',
    [AdminForthFilterOperators.IS_EMPTY]: 'IS NULL',
    [AdminForthFilterOperators.IS_NOT_EMPTY]: 'IS NOT NULL',
  };

  SortDirectionsMap = {
    [AdminForthSortDirections.asc]: 'ASC',
    [AdminForthSortDirections.desc]: 'DESC',
  };

  async setupClient(url: string): Promise<void> {
    const databasePath = decodeURIComponent(url.replace(DUCKDB_URL_PREFIX_RE, ''));
    this.instance = await DuckDBInstance.create(databasePath);
    this.client = await this.instance.connect();
  }

  private async rows(sql: string, values: DuckDBValue[] = []): Promise<DuckDBRow[]> {
    const reader = await this.client.runAndReadAll(sql, values);
    return reader.getRowObjectsJson() as DuckDBRow[];
  }

  private validateColumnNames(resource: AdminForthResource, columns: string[]): void {
    const knownColumns = new Set(resource.dataSourceColumns.map((column) => column.name));
    const unknownColumn = columns.find((column) => !knownColumns.has(column));
    if (unknownColumn) {
      throw new Error(`Invalid column name: ${unknownColumn}`);
    }
  }

  async getAllTables(): Promise<string[]> {
    const rows = await this.rows(`
      SELECT table_name
      FROM information_schema.tables
      WHERE table_schema = 'main' AND table_type = 'BASE TABLE'
      ORDER BY table_name
    `);
    return rows.map((row) => row.table_name);
  }

  async getAllColumnsInTable(tableName: string): Promise<Array<{
    name: string;
    type?: string;
    isPrimaryKey?: boolean;
    sampleValue?: any;
  }>> {
    const { schema, name } = tableParts(tableName);
    const columns = await this.rows(`
      SELECT
        c.column_name,
        c.data_type,
        EXISTS (
          SELECT 1
          FROM information_schema.table_constraints tc
          JOIN information_schema.key_column_usage kcu
            ON tc.constraint_catalog = kcu.constraint_catalog
            AND tc.constraint_schema = kcu.constraint_schema
            AND tc.constraint_name = kcu.constraint_name
          WHERE tc.constraint_type = 'PRIMARY KEY'
            AND tc.table_schema = c.table_schema
            AND tc.table_name = c.table_name
            AND kcu.column_name = c.column_name
        ) AS is_primary_key
      FROM information_schema.columns c
      WHERE c.table_schema = $1 AND c.table_name = $2
      ORDER BY c.ordinal_position
    `, [schema, name]);
    const sampleRows = await this.rows(`SELECT * FROM ${quoteIdentifier(tableName)} LIMIT 1`);
    const sampleRow = sampleRows[0] ?? {};
    return columns.map((column) => ({
      name: column.column_name,
      type: column.data_type,
      isPrimaryKey: column.is_primary_key,
      sampleValue: sampleRow[column.column_name],
    }));
  }

  async isDatabaseEmpty(): Promise<boolean> {
    const rows = await this.rows(`
      SELECT 1
      FROM information_schema.tables
      WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        AND table_type = 'BASE TABLE'
      LIMIT 1
    `);
    return rows.length === 0;
  }

  async discoverFields(
    resource: AdminForthResource,
    _config: AdminForthConfig,
  ): Promise<{ [key: string]: AdminForthResourceColumn }> {
    const { schema, name } = tableParts(resource.table);
    const rows = await this.rows(`
      SELECT
        c.column_name AS name,
        c.data_type AS type,
        c.is_nullable = 'NO' AS not_null,
        c.column_default,
        EXISTS (
          SELECT 1
          FROM information_schema.table_constraints tc
          JOIN information_schema.key_column_usage kcu
            ON tc.constraint_catalog = kcu.constraint_catalog
            AND tc.constraint_schema = kcu.constraint_schema
            AND tc.constraint_name = kcu.constraint_name
          WHERE tc.constraint_type = 'PRIMARY KEY'
            AND tc.table_schema = c.table_schema
            AND tc.table_name = c.table_name
            AND kcu.column_name = c.column_name
        ) AS primary_key
      FROM information_schema.columns c
      WHERE c.table_schema = $1 AND c.table_name = $2
      ORDER BY c.ordinal_position
    `, [schema, name]);

    return Object.fromEntries(rows.map((row) => [
      row.name,
      {
        ...typeMetadata(row.type),
        _baseTypeDebug: row.type,
        primaryKey: row.primary_key,
        default: row.column_default,
        required: row.not_null && !row.column_default,
      },
    ])) as { [key: string]: AdminForthResourceColumn };
  }

  getFieldValue(field: AdminForthResourceColumn, value: any): any {
    if (value === null || value === undefined) {
      return null;
    }
    if (field.type === AdminForthDataTypes.DATETIME) {
      const normalizedValue = typeof value === 'string'
        && value.includes(' ')
        && !TIMESTAMP_TIMEZONE_SUFFIX_RE.test(value)
        ? `${value.replace(' ', 'T')}Z`
        : value;
      return dayjs(normalizedValue).toISOString();
    }
    if (field.type === AdminForthDataTypes.DATE) {
      return String(value).slice(0, 10);
    }
    if (field.type === AdminForthDataTypes.JSON && typeof value === 'string') {
      return JSON.parse(value);
    }
    return value;
  }

  setFieldValue(field: AdminForthResourceColumn, value: any): any {
    if (value === null || value === undefined) {
      return null;
    }
    if (field.type === AdminForthDataTypes.DATETIME) {
      return dayjs(value).toISOString();
    }
    if (field.isArray?.enabled || field.type === AdminForthDataTypes.JSON) {
      return typeof value === 'string' ? value : JSON.stringify(value);
    }
    return value;
  }

  getFilterString(
    resource: AdminForthResource,
    filter: IAdminForthSingleFilter | IAdminForthAndOrFilter,
  ): string {
    const singleFilter = filter as IAdminForthSingleFilter;
    if (singleFilter.field) {
      const operator = this.OperatorsMap[filter.operator as keyof typeof this.OperatorsMap];
      const field = quoteIdentifier(singleFilter.field);
      if (singleFilter.rightField) {
        return `${field} ${operator} ${quoteIdentifier(singleFilter.rightField)}`;
      }
      if (filter.operator === AdminForthFilterOperators.IS_EMPTY || filter.operator === AdminForthFilterOperators.IS_NOT_EMPTY) {
        return `${field} ${operator}`;
      }
      if (filter.operator === AdminForthFilterOperators.EQ && filter.value === null) {
        return `${field} IS NULL`;
      }

      let comparedField = field;
      const fieldData = resource.dataSourceColumns.find((column) => column.name === singleFilter.field);
      const isTextFilter = filter.operator === AdminForthFilterOperators.LIKE
        || filter.operator === AdminForthFilterOperators.ILIKE;
      if (isTextFilter && (
        fieldData?._underlineType === 'uuid'
        || fieldData?.type === AdminForthDataTypes.JSON
        || fieldData?.isArray?.enabled
      )
      ) {
        comparedField = `CAST(${field} AS VARCHAR)`;
      }
      if (filter.operator === AdminForthFilterOperators.IN || filter.operator === AdminForthFilterOperators.NIN) {
        return `${comparedField} ${operator} (${filter.value.map(() => '$?').join(', ')})`;
      }
      return `${comparedField} ${operator} $?`;
    }
    if (singleFilter.insecureRawSQL) {
      return singleFilter.insecureRawSQL;
    }
    const compoundFilter = filter as IAdminForthAndOrFilter;
    return compoundFilter.subFilters
      .map((subFilter) => {
        const sql = this.getFilterString(resource, subFilter);
        return (subFilter as IAdminForthAndOrFilter).subFilters ? `(${sql})` : sql;
      })
      .join(` ${this.OperatorsMap[compoundFilter.operator]} `);
  }

  getFilterParams(filter: IAdminForthSingleFilter | IAdminForthAndOrFilter): DuckDBValue[] {
    const singleFilter = filter as IAdminForthSingleFilter;
    if (singleFilter.field) {
      if (singleFilter.rightField
        || filter.operator === AdminForthFilterOperators.IS_EMPTY
        || filter.operator === AdminForthFilterOperators.IS_NOT_EMPTY
        || (filter.operator === AdminForthFilterOperators.EQ && filter.value === null)) {
        return [];
      }
      if (filter.operator === AdminForthFilterOperators.LIKE || filter.operator === AdminForthFilterOperators.ILIKE) {
        return [`%${filter.value}%`];
      }
      if (filter.operator === AdminForthFilterOperators.IN || filter.operator === AdminForthFilterOperators.NIN) {
        return filter.value.map(normalizeBoundValue);
      }
      return [normalizeBoundValue(singleFilter.value)];
    }
    if (singleFilter.insecureRawSQL) {
      return [];
    }
    return (filter as IAdminForthAndOrFilter).subFilters.flatMap((subFilter) => this.getFilterParams(subFilter));
  }

  whereClauseAndValues(resource: AdminForthResource, filters: IAdminForthAndOrFilter): {
    sql: string;
    nextParameter: number;
    values: DuckDBValue[];
  } {
    let sql = filters.subFilters.length ? `WHERE ${this.getFilterString(resource, filters)}` : '';
    const values = filters.subFilters.length ? this.getFilterParams(filters) : [];
    values.forEach((_, index) => {
      sql = sql.replace('$?', `$${index + 1}`);
    });
    return { sql, nextParameter: values.length + 1, values };
  }

  async getAggregateWithOriginalTypes({ resource, filters, aggregations, groupBy }: {
    resource: AdminForthResource;
    filters: IAdminForthAndOrFilter;
    aggregations: { [alias: string]: IAggregationRule };
    groupBy?: IGroupByRule | IGroupByRule[];
  }): Promise<Array<{ group?: string; [key: string]: any }>> {
    const selectParts: string[] = [];
    const groupExpressions: string[] = [];
    const groupByRules = this.normalizeGroupByRules(groupBy);

    groupByRules.forEach((groupByRule, index) => {
      let expression: string;
      if (groupByRule.type === 'date_trunc') {
        const rule = groupByRule as IGroupByDateTrunc;
        const column = resource.dataSourceColumns.find((item) => item.name === rule.field);
        const field = quoteIdentifier(rule.field);
        const timezone = rule.timezone ?? 'UTC';
        const isTimestampWithTimezone = column?._underlineType === 'timestamp with time zone'
          || column?._underlineType === 'timestamptz';
        const localizedField = isTimestampWithTimezone
          ? `TIMEZONE('${timezone}', ${field})`
          : `TIMEZONE('${timezone}', TIMEZONE('UTC', ${field}))`;
        expression = `CAST(DATE_TRUNC('${rule.truncation}', ${localizedField}) AS DATE)`;
      } else {
        expression = quoteIdentifier((groupByRule as IGroupByField).field);
      }
      groupExpressions.push(expression);
      selectParts.push(`${expression} AS ${quoteIdentifier(this.getGroupByResultAlias(groupByRule, index, groupByRules.length))}`);
    });

    for (const [alias, rule] of Object.entries(aggregations)) {
      const field = rule.field ? quoteIdentifier(rule.field) : '';
      switch (rule.operation) {
        case 'sum': selectParts.push(`SUM(${field}) AS ${quoteIdentifier(alias)}`); break;
        case 'count': selectParts.push(`COUNT(*) AS ${quoteIdentifier(alias)}`); break;
        case 'count_distinct': selectParts.push(`COUNT(DISTINCT ${field}) AS ${quoteIdentifier(alias)}`); break;
        case 'avg': selectParts.push(`AVG(${field}) AS ${quoteIdentifier(alias)}`); break;
        case 'min': selectParts.push(`MIN(${field}) AS ${quoteIdentifier(alias)}`); break;
        case 'max': selectParts.push(`MAX(${field}) AS ${quoteIdentifier(alias)}`); break;
        case 'median': selectParts.push(`MEDIAN(${field}) AS ${quoteIdentifier(alias)}`); break;
      }
    }

    const { sql: where, values } = this.whereClauseAndValues(resource, filters);
    let sql = `SELECT ${selectParts.join(', ')} FROM ${quoteIdentifier(resource.table)} ${where}`;
    if (groupExpressions.length) {
      sql += ` GROUP BY ${groupExpressions.join(', ')} ORDER BY ${groupExpressions.join(', ')} ASC`;
    }
    dbLogger.trace(`🪲📜 DUCKDB AGG Q: ${sql}, params: ${JSON.stringify(values)}`);
    return this.rows(sql, values);
  }

  async getDataWithOriginalTypes({ resource, limit, offset, sort, filters, columns }: {
    resource: AdminForthResource;
    limit: number;
    offset: number;
    sort: IAdminForthSort[];
    filters: IAdminForthAndOrFilter;
    columns?: AdminForthResourceColumn[];
  }): Promise<DuckDBRow[]> {
    if (sort.some((item) => !checkIfFieldIsInsideResourceColumns(item.field, resource))) {
      const invalidSort = sort.find((item) => !checkIfFieldIsInsideResourceColumns(item.field, resource));
      throw new Error(`Invalid sort field: ${invalidSort?.field}`);
    }

    const selectedColumns = (columns ?? resource.dataSourceColumns)
      .map((column) => quoteIdentifier(column.name))
      .join(', ');
    const { sql: where, nextParameter, values } = this.whereClauseAndValues(resource, filters);
    const orderBy = sort.length
      ? `ORDER BY ${sort.map((item) => `${quoteIdentifier(item.field)} ${this.SortDirectionsMap[item.direction]}`).join(', ')}`
      : '';
    const sql = `SELECT ${selectedColumns} FROM ${quoteIdentifier(resource.table)} ${where} ${orderBy} LIMIT $${nextParameter} OFFSET $${nextParameter + 1}`;
    const params = [...values, limit, offset];
    dbLogger.trace(`🪲📜 DUCKDB Q: ${sql}, params: ${JSON.stringify(params)}`);
    return this.rows(sql, params);
  }

  async getCount({ resource, filters }: {
    resource: AdminForthResource;
    filters: IAdminForthAndOrFilter;
  }): Promise<number> {
    let normalizedFilters = filters;
    if (filters) {
      const validation = this.validateAndNormalizeFilters(filters, resource);
      if (!validation.ok) {
        throw new Error(validation.error);
      }
      normalizedFilters = validation.normalizedFilters as IAdminForthAndOrFilter;
    }
    const { sql: where, values } = this.whereClauseAndValues(resource, normalizedFilters);
    const sql = `SELECT COUNT(*) AS count FROM ${quoteIdentifier(resource.table)} ${where}`;
    dbLogger.trace(`🪲📜 DUCKDB Q: ${sql}, params: ${JSON.stringify(values)}`);
    const rows = await this.rows(sql, values);
    return Number(rows[0].count);
  }

  async getMinMaxForColumnsWithOriginalTypes({ resource, columns }: {
    resource: AdminForthResource;
    columns: AdminForthResourceColumn[];
  }): Promise<{ [key: string]: { min: any; max: any } }> {
    const result: { [key: string]: { min: any; max: any } } = {};
    const select = columns.flatMap((column, index) => [
      `MIN(${quoteIdentifier(column.name)}) AS ${quoteIdentifier(`min_${index}`)}`,
      `MAX(${quoteIdentifier(column.name)}) AS ${quoteIdentifier(`max_${index}`)}`,
    ]).join(', ');
    const rows = await this.rows(`SELECT ${select} FROM ${quoteIdentifier(resource.table)}`);
    columns.forEach((column, index) => {
      result[column.name] = {
        min: rows[0][`min_${index}`],
        max: rows[0][`max_${index}`],
      };
    });
    return result;
  }

  async createRecordOriginalValues({ resource, record }: {
    resource: AdminForthResource;
    record: Record<string, any>;
  }): Promise<string> {
    const primaryKey = this.getPrimaryKey(resource);
    const columnNames = Object.keys(record);
    this.validateColumnNames(resource, columnNames);
    const values = columnNames.map((columnName) => normalizeBoundValue(record[columnName]));
    const insert = columnNames.length
      ? `(${columnNames.map(quoteIdentifier).join(', ')}) VALUES (${columnNames.map((_, index) => `$${index + 1}`).join(', ')})`
      : 'DEFAULT VALUES';
    const sql = `INSERT INTO ${quoteIdentifier(resource.table)} ${insert} RETURNING ${quoteIdentifier(primaryKey)}`;
    dbLogger.trace(`🪲📜 DUCKDB Q: ${sql}, params: ${JSON.stringify(values)}`);
    const rows = await this.rows(sql, values);
    return String(rows[0][primaryKey]);
  }

  async updateRecordOriginalValues({ resource, recordId, newValues }: {
    resource: AdminForthResource;
    recordId: any;
    newValues: Record<string, any>;
  }): Promise<void> {
    const columns = Object.keys(newValues);
    this.validateColumnNames(resource, columns);
    const values = columns.map((column) => normalizeBoundValue(newValues[column]));
    values.push(normalizeBoundValue(recordId));
    const assignments = columns.map((column, index) => `${quoteIdentifier(column)} = $${index + 1}`).join(', ');
    const sql = `UPDATE ${quoteIdentifier(resource.table)} SET ${assignments} WHERE ${quoteIdentifier(this.getPrimaryKey(resource))} = $${values.length}`;
    dbLogger.trace(`🪲📜 DUCKDB Q: ${sql}, params: ${JSON.stringify(values)}`);
    await this.client.run(sql, values);
  }

  async deleteRecord({ resource, recordId }: {
    resource: AdminForthResource;
    recordId: any;
  }): Promise<boolean> {
    const sql = `DELETE FROM ${quoteIdentifier(resource.table)} WHERE ${quoteIdentifier(this.getPrimaryKey(resource))} = $1`;
    dbLogger.trace(`🪲📜 DUCKDB Q: ${sql}, params: ${JSON.stringify([recordId])}`);
    const result = await this.client.run(sql, [normalizeBoundValue(recordId)]);
    return result.rowsChanged > 0;
  }

  async deleteMany({ resource, recordIds }: {
    resource: AdminForthResource;
    recordIds: string[];
  }): Promise<number> {
    if (!recordIds.length) {
      return 0;
    }
    const placeholders = recordIds.map((_, index) => `$${index + 1}`).join(', ');
    const sql = `DELETE FROM ${quoteIdentifier(resource.table)} WHERE ${quoteIdentifier(this.getPrimaryKey(resource))} IN (${placeholders})`;
    const values = recordIds.map(normalizeBoundValue);
    dbLogger.trace(`🪲📜 DUCKDB Q: ${sql}, params: ${JSON.stringify(values)}`);
    const result = await this.client.run(sql, values);
    return result.rowsChanged;
  }

  async close(): Promise<void> {
    this.client.closeSync();
    this.instance?.closeSync();
  }
}

export default DuckDBConnector;
