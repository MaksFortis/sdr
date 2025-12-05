package schema_database

import (
	"database/sql"
	"fmt"
	"strings"

	"crm-lead-service/internal/domain"
)

type SchemaService struct {
	db *sql.DB
}

func NewSchemaService(db *sql.DB) *SchemaService {
	return &SchemaService{db: db}
}

// GetTableColumns получает информацию о колонках таблицы из БД
func (s *SchemaService) GetTableColumns(tableName string) (map[string]domain.ColumnInfo, error) {
	query := `
		SELECT 
			column_name,
			data_type,
			is_nullable,
			column_default,
			character_maximum_length
		FROM information_schema.columns
		WHERE table_name = $1
		ORDER BY ordinal_position
	`

	rows, err := s.db.Query(query, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query columns: %w", err)
	}
	defer rows.Close()

	columns := make(map[string]domain.ColumnInfo)
	for rows.Next() {
		var (
			columnName string
			dataType   string
			isNullable string
			colDefault sql.NullString
			maxLength  sql.NullInt64
		)

		if err := rows.Scan(&columnName, &dataType, &isNullable, &colDefault, &maxLength); err != nil {
			return nil, fmt.Errorf("failed to scan column: %w", err)
		}

		var size *int
		if maxLength.Valid {
			s := int(maxLength.Int64)
			size = &s
		}

		columns[columnName] = domain.ColumnInfo{
			Name:      columnName,
			DbType:    dataType,
			AllowNull: isNullable == "YES",
			Size:      size,
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}

	return columns, nil
}

func (s *SchemaService) TableExists(tableName string) (bool, error) {
	query := `
		SELECT EXISTS (
			SELECT FROM information_schema.tables 
			WHERE table_name = $1
		)
	`

	var exists bool
	err := s.db.QueryRow(query, tableName).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check table existence: %w", err)
	}

	return exists, nil
}

func (s *SchemaService) CompareSchemas(messageSchema domain.Schema) (bool, []string, error) {
	tableName := messageSchema.TableName

	exists, err := s.TableExists(tableName)
	if err != nil {
		return false, nil, err
	}

	if !exists {
		return false, nil, nil
	}

	dbColumns, err := s.GetTableColumns(tableName)
	if err != nil {
		return false, nil, err
	}

	var missingColumns []string
	for columnName := range messageSchema.Columns {
		if _, exists := dbColumns[columnName]; !exists {
			missingColumns = append(missingColumns, columnName)
		}
	}

	isEqual := len(missingColumns) == 0
	return isEqual, missingColumns, nil
}

// AddColumns добавляет недостающие колонки в таблицу
func (s *SchemaService) AddColumns(tableName string, columns map[string]domain.ColumnInfo, missingColumns []string) error {
	for _, columnName := range missingColumns {
		column, exists := columns[columnName]
		if !exists {
			continue
		}

		columnType := s.mapTypeToPostgres(column)
		nullable := "NULL"
		if !column.AllowNull {
			nullable = "NOT NULL"
		}

		// Экранируем имена таблицы и колонки в двойные кавычки
		query := fmt.Sprintf(`ALTER TABLE "%s" ADD COLUMN IF NOT EXISTS "%s" %s %s`,
			tableName, columnName, columnType, nullable)

		_, err := s.db.Exec(query)
		if err != nil {
			return fmt.Errorf("failed to add column %s: %w", columnName, err)
		}
	}

	return nil
}

// CreateTable создает таблицу на основе схемы из сообщения
func (s *SchemaService) CreateTable(schema domain.Schema) error {
	var columnDefs []string

	for columnName, column := range schema.Columns {
		columnType := s.mapTypeToPostgres(column)
		nullable := "NULL"
		if !column.AllowNull {
			nullable = "NOT NULL"
		}

		// Экранируем имя колонки в двойные кавычки для защиты от зарезервированных слов
		def := fmt.Sprintf(`"%s" %s %s`, columnName, columnType, nullable)

		if column.DefaultValue != nil {
			defaultVal := formatDefaultValue(column.DefaultValue)
			if defaultVal != "" {
				def += fmt.Sprintf(" DEFAULT %s", defaultVal)
			}
		} else if !column.AllowNull {
			// Добавляем дефолтное значение для NOT NULL колонок без explicit default
			def += getDefaultForType(columnType)
		}

		columnDefs = append(columnDefs, def)
	}

	// Добавляем PRIMARY KEY с экранированными именами колонок
	if len(schema.PrimaryKey) > 0 {
		quotedPK := make([]string, len(schema.PrimaryKey))
		for i, pk := range schema.PrimaryKey {
			quotedPK[i] = fmt.Sprintf(`"%s"`, pk)
		}
		pkDef := fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(quotedPK, ", "))
		columnDefs = append(columnDefs, pkDef)
	}

	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s" (%s)`,
		schema.TableName, strings.Join(columnDefs, ", "))

	_, err := s.db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	return nil
}

// getDefaultForType возвращает дефолтное значение для типа данных
func getDefaultForType(columnType string) string {
	upperType := strings.ToUpper(columnType)
	switch {
	case strings.Contains(upperType, "INT"):
		return " DEFAULT 0"
	case strings.Contains(upperType, "BOOL"):
		return " DEFAULT false"
	case strings.Contains(upperType, "NUMERIC"), strings.Contains(upperType, "DECIMAL"),
		strings.Contains(upperType, "DOUBLE"), strings.Contains(upperType, "FLOAT"):
		return " DEFAULT 0"
	case strings.Contains(upperType, "VARCHAR"), strings.Contains(upperType, "TEXT"),
		strings.Contains(upperType, "CHAR"):
		return " DEFAULT ''"
	case strings.Contains(upperType, "TIMESTAMP"):
		return " DEFAULT CURRENT_TIMESTAMP"
	default:
		return ""
	}
}

// formatDefaultValue форматирует значение по умолчанию для SQL
func formatDefaultValue(value interface{}) string {
	if value == nil {
		return ""
	}

	// Проверяем, является ли значение map с expression (например, CURRENT_TIMESTAMP)
	if m, ok := value.(map[string]interface{}); ok {
		if expr, exists := m["expression"]; exists {
			return fmt.Sprintf("%v", expr)
		}
		// Если это другой map, пропускаем
		return ""
	}

	// Обрабатываем простые типы
	switch v := value.(type) {
	case string:
		return fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''"))
	case bool:
		if v {
			return "true"
		}
		return "false"
	case float64, float32, int, int32, int64:
		return fmt.Sprintf("%v", v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// mapTypeToPostgres преобразует тип из схемы в тип PostgreSQL
func (s *SchemaService) mapTypeToPostgres(column domain.ColumnInfo) string {
	switch column.Type {
	case "bigint":
		return "BIGINT"
	case "integer", "smallint":
		return strings.ToUpper(column.Type)
	case "string":
		if column.Size != nil {
			return fmt.Sprintf("VARCHAR(%d)", *column.Size)
		}
		return "TEXT"
	case "text":
		return "TEXT"
	case "boolean":
		return "BOOLEAN"
	case "date":
		return "DATE"
	case "timestamp":
		return "TIMESTAMPTZ"
	case "double":
		return "DOUBLE PRECISION"
	default:
		// По умолчанию используем dbType
		if column.DbType != "" {
			return strings.ToUpper(column.DbType)
		}
		return "TEXT"
	}
}
