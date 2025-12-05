package domain

import "encoding/json"

type EventTypeEnum string

const (
	EventTypeInsert EventTypeEnum = "insert"
	EventTypeUpdate EventTypeEnum = "update"
)

type Message struct {
	EventType EventTypeEnum `json:"event_type"`
	Data      []Fields      `json:"data"`
	Schema    Schema        `json:"schema"`
}

// Fields аттрибуты модели
type Fields struct {
	Field    string      `json:"field"`
	OldValue interface{} `json:"old_value,omitempty"`
	NewValue interface{} `json:"new_value"`
}

type Schema struct {
	TableName  string                `json:"tableName"`
	Columns    map[string]ColumnInfo `json:"columns"`
	PrimaryKey []string              `json:"primaryKey"`
}

// ColumnInfo представляет информацию о колонке
type ColumnInfo struct {
	Dimension                               int         `json:"dimension"`
	DisableJSONSupport                      bool        `json:"disableJsonSupport"`
	DisableArraySupport                     bool        `json:"disableArraySupport"`
	DeserializeArrayColumnToArrayExpression bool        `json:"deserializeArrayColumnToArrayExpression"`
	SequenceName                            *string     `json:"sequenceName"`
	Name                                    string      `json:"name"`
	AllowNull                               bool        `json:"allowNull"`
	Type                                    string      `json:"type"`
	PhpType                                 string      `json:"phpType"`
	DbType                                  string      `json:"dbType"`
	DefaultValue                            interface{} `json:"defaultValue"`
	EnumValues                              []string    `json:"enumValues"`
	Size                                    *int        `json:"size"`
	Precision                               *int        `json:"precision"`
	Scale                                   *int        `json:"scale"`
	IsPrimaryKey                            bool        `json:"isPrimaryKey"`
	AutoIncrement                           bool        `json:"autoIncrement"`
	Unsigned                                bool        `json:"unsigned"`
	Comment                                 *string     `json:"comment"`
}

func NewMessage(data []byte) (*Message, error) {
	var msg Message
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}

func (m *Message) ValidateMessage() (bool, error) {
	if m.Schema.TableName == "" {
		return false, nil
	}
	if len(m.Schema.Columns) == 0 {
		return false, nil
	}
	return true, nil
}

func (m *Message) GetFieldValue(fieldName string) (interface{}, bool) {
	for _, change := range m.Data {
		if change.Field == fieldName {
			return change.NewValue, true
		}
	}
	return nil, false
}
