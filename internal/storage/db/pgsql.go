package db

import (
	"fmt"
	"strings"

	"crm-lead-service/internal/domain"
	"crm-lead-service/internal/service/schema_database"
	"crm-lead-service/pkg/database"
)

type Storage struct {
	Conn          *database.ConnectionDatabase
	SchemaService *schema_database.SchemaService
}

func NewStorage(db *database.ConnectionDatabase) (*Storage, error) {
	return &Storage{
		Conn:          db,
		SchemaService: schema_database.NewSchemaService(db.DB),
	}, nil
}

func (s *Storage) SaveMessage(message *domain.Message) error {
	tableName := message.Schema.TableName

	if err := s.CheckAndUpdateSchema(message.Schema); err != nil {
		return fmt.Errorf("failed to check/update schema: %w", err)
	}

	// Определяем тип операции
	switch message.EventType {
	case domain.EventTypeInsert:
		return s.InsertData(tableName, message.Data)
	case domain.EventTypeUpdate:
		return s.UpdateData(tableName, message.Data, message.Schema.PrimaryKey)
	default:
		return fmt.Errorf("unknown event type: %s", message.EventType)
	}
}

// CheckAndUpdateSchema проверяет и обновляет схему таблицы
func (s *Storage) CheckAndUpdateSchema(schema domain.Schema) error {
	tableName := schema.TableName

	// Проверяем существование таблицы
	exists, err := s.SchemaService.TableExists(tableName)
	if err != nil {
		return err
	}

	// Если таблицы нет - создаем
	if !exists {
		return s.SchemaService.CreateTable(schema)
	}

	// Сравниваем схемы
	isEqual, missingColumns, err := s.SchemaService.CompareSchemas(schema)
	if err != nil {
		return err
	}

	// Если схемы не совпадают - добавляем недостающие колонки
	if !isEqual {
		return s.SchemaService.AddColumns(tableName, schema.Columns, missingColumns)
	}

	return nil
}

func (s *Storage) InsertData(tableName string, data []domain.Fields) error {
	if len(data) == 0 {
		return nil
	}

	var columns []string
	var values []interface{}
	var placeholders []string
	paramIndex := 1

	for _, field := range data {
		// Пропускаем NULL значения, чтобы использовались DEFAULT из схемы
		if field.NewValue == nil {
			continue
		}
		// Экранируем имя колонки в двойные кавычки
		columns = append(columns, fmt.Sprintf(`"%s"`, field.Field))
		values = append(values, field.NewValue)
		placeholders = append(placeholders, fmt.Sprintf("$%d", paramIndex))
		paramIndex++
	}

	if len(columns) == 0 {
		return nil
	}

	query := fmt.Sprintf(
		`INSERT INTO "%s" (%s) VALUES (%s) ON CONFLICT DO NOTHING`,
		tableName,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)

	_, err := s.Conn.DB.Exec(query, values...)
	if err != nil {
		return fmt.Errorf("failed to insert data: %w", err)
	}

	return nil
}

// UpdateData обновляет данные в таблице
func (s *Storage) UpdateData(tableName string, data []domain.Fields, primaryKeys []string) error {
	if len(data) == 0 {
		return nil
	}

	var setClauses []string
	var whereClause []string
	var values []interface{}
	valueIndex := 1

	// Получаем значения первичных ключей
	pkValues := make(map[string]interface{})
	for _, field := range data {
		for _, pk := range primaryKeys {
			if field.Field == pk {
				pkValues[pk] = field.NewValue
			}
		}
	}

	// Формируем SET часть запроса
	for _, field := range data {
		// Пропускаем первичные ключи в SET
		isPrimaryKey := false
		for _, pk := range primaryKeys {
			if field.Field == pk {
				isPrimaryKey = true
				break
			}
		}

		if !isPrimaryKey && field.NewValue != nil {
			// Экранируем имя колонки в двойные кавычки
			setClauses = append(setClauses, fmt.Sprintf(`"%s" = $%d`, field.Field, valueIndex))
			values = append(values, field.NewValue)
			valueIndex++
		}
	}

	// Формируем WHERE часть запроса
	for _, pk := range primaryKeys {
		if val, exists := pkValues[pk]; exists {
			// Экранируем имя колонки в двойные кавычки
			whereClause = append(whereClause, fmt.Sprintf(`"%s" = $%d`, pk, valueIndex))
			values = append(values, val)
			valueIndex++
		}
	}

	if len(setClauses) == 0 {
		return fmt.Errorf("no fields to update")
	}

	if len(whereClause) == 0 {
		return fmt.Errorf("no primary key values found")
	}

	query := fmt.Sprintf(
		`UPDATE "%s" SET %s WHERE %s`,
		tableName,
		strings.Join(setClauses, ", "),
		strings.Join(whereClause, " AND "),
	)

	result, err := s.Conn.DB.Exec(query, values...)
	if err != nil {
		return fmt.Errorf("failed to update data: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		// Если запись не найдена, пытаемся вставить
		return s.InsertData(tableName, data)
	}

	return nil
}
