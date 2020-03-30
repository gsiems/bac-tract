package bactract

// Read/parse the bacpac model.xml file and extract the information
// needed for parsing the BCP data files.

import (
	"encoding/json"
	"encoding/xml"
	"io/ioutil"
	"os"
	"strings"

	//
	"golang.org/x/net/html/charset"
)

// DataSchemaModel is for containing the contents of the model.xml
// file. Structure generated using https://github.com/miku/zek/
// ~/go/bin/zek -p < extracted/model.xml > model.go
type DataSchemaModel struct {
	XMLName                xml.Name `xml:"DataSchemaModel"`
	Text                   string   `xml:",chardata"`
	FileFormatVersion      string   `xml:"FileFormatVersion,attr"`
	SchemaVersion          string   `xml:"SchemaVersion,attr"`
	DspName                string   `xml:"DspName,attr"`
	CollationLcid          string   `xml:"CollationLcid,attr"`
	CollationCaseSensitive string   `xml:"CollationCaseSensitive,attr"`
	Xmlns                  string   `xml:"xmlns,attr"`
	Model                  struct {
		Text    string `xml:",chardata"`
		Element []struct {
			Text          string `xml:",chardata"`
			Type          string `xml:"Type,attr"`
			Disambiguator string `xml:"Disambiguator,attr"`
			Name          string `xml:"Name,attr"`
			Property      []struct {
				Text      string `xml:",chardata"`
				Name      string `xml:"Name,attr"`
				AttrValue string `xml:"Value,attr"`
				Value     struct {
					Text              string `xml:",chardata"`
					QuotedIdentifiers string `xml:"QuotedIdentifiers,attr"`
					AnsiNulls         string `xml:"AnsiNulls,attr"`
				} `xml:"Value"`
			} `xml:"Property"`
			Relationship []struct {
				Text  string `xml:",chardata"`
				Name  string `xml:"Name,attr"`
				Entry []struct {
					Text       string `xml:",chardata"`
					References struct {
						Text           string `xml:",chardata"`
						Name           string `xml:"Name,attr"`
						ExternalSource string `xml:"ExternalSource,attr"`
						Disambiguator  string `xml:"Disambiguator,attr"`
					} `xml:"References"`
					Element struct {
						Text         string `xml:",chardata"`
						Type         string `xml:"Type,attr"`
						Name         string `xml:"Name,attr"`
						Relationship struct {
							Text  string `xml:",chardata"`
							Name  string `xml:"Name,attr"`
							Entry []struct {
								Text       string `xml:",chardata"`
								References struct {
									Text           string `xml:",chardata"`
									Name           string `xml:"Name,attr"`
									ExternalSource string `xml:"ExternalSource,attr"`
								} `xml:"References"`
								Element struct {
									Text     string `xml:",chardata"`
									Type     string `xml:"Type,attr"`
									Name     string `xml:"Name,attr"`
									Property []struct {
										Text  string `xml:",chardata"`
										Name  string `xml:"Name,attr"`
										Value string `xml:"Value,attr"`
									} `xml:"Property"`
									Relationship struct {
										Text  string `xml:",chardata"`
										Name  string `xml:"Name,attr"`
										Entry struct {
											Text       string `xml:",chardata"`
											References struct {
												Text           string `xml:",chardata"`
												ExternalSource string `xml:"ExternalSource,attr"`
												Name           string `xml:"Name,attr"`
											} `xml:"References"`
											Element struct {
												Text         string `xml:",chardata"`
												Type         string `xml:"Type,attr"`
												Relationship struct {
													Text  string `xml:",chardata"`
													Name  string `xml:"Name,attr"`
													Entry struct {
														Text       string `xml:",chardata"`
														References struct {
															Text           string `xml:",chardata"`
															ExternalSource string `xml:"ExternalSource,attr"`
															Name           string `xml:"Name,attr"`
														} `xml:"References"`
													} `xml:"Entry"`
												} `xml:"Relationship"`
												Property []struct {
													Text  string `xml:",chardata"`
													Name  string `xml:"Name,attr"`
													Value string `xml:"Value,attr"`
												} `xml:"Property"`
											} `xml:"Element"`
											Annotation []struct {
												Text     string `xml:",chardata"`
												Type     string `xml:"Type,attr"`
												Name     string `xml:"Name,attr"`
												Property []struct {
													Text  string `xml:",chardata"`
													Name  string `xml:"Name,attr"`
													Value string `xml:"Value,attr"`
												} `xml:"Property"`
											} `xml:"Annotation"`
										} `xml:"Entry"`
									} `xml:"Relationship"`
								} `xml:"Element"`
								Annotation []struct {
									Text     string `xml:",chardata"`
									Type     string `xml:"Type,attr"`
									Name     string `xml:"Name,attr"`
									Property []struct {
										Text  string `xml:",chardata"`
										Name  string `xml:"Name,attr"`
										Value string `xml:"Value,attr"`
									} `xml:"Property"`
								} `xml:"Annotation"`
							} `xml:"Entry"`
						} `xml:"Relationship"`
						Property []struct {
							Text      string `xml:",chardata"`
							Name      string `xml:"Name,attr"`
							AttrValue string `xml:"Value,attr"`
							Value     struct {
								Text              string `xml:",chardata"`
								QuotedIdentifiers string `xml:"QuotedIdentifiers,attr"`
								AnsiNulls         string `xml:"AnsiNulls,attr"`
							} `xml:"Value"`
						} `xml:"Property"`
						AttachedAnnotation struct {
							Text          string `xml:",chardata"`
							Disambiguator string `xml:"Disambiguator,attr"`
						} `xml:"AttachedAnnotation"`
						Annotation struct {
							Text     string `xml:",chardata"`
							Type     string `xml:"Type,attr"`
							Property []struct {
								Text  string `xml:",chardata"`
								Name  string `xml:"Name,attr"`
								Value string `xml:"Value,attr"`
							} `xml:"Property"`
						} `xml:"Annotation"`
					} `xml:"Element"`
					Annotation []struct {
						Text     string `xml:",chardata"`
						Type     string `xml:"Type,attr"`
						Name     string `xml:"Name,attr"`
						Property []struct {
							Text  string `xml:",chardata"`
							Name  string `xml:"Name,attr"`
							Value string `xml:"Value,attr"`
						} `xml:"Property"`
					} `xml:"Annotation"`
				} `xml:"Entry"`
			} `xml:"Relationship"`
			Annotation []struct {
				Text          string `xml:",chardata"`
				Type          string `xml:"Type,attr"`
				Name          string `xml:"Name,attr"`
				Disambiguator string `xml:"Disambiguator,attr"`
				Property      []struct {
					Text  string `xml:",chardata"`
					Name  string `xml:"Name,attr"`
					Value string `xml:"Value,attr"`
				} `xml:"Property"`
			} `xml:"Annotation"`
			AttachedAnnotation []struct {
				Text          string `xml:",chardata"`
				Disambiguator string `xml:"Disambiguator,attr"`
			} `xml:"AttachedAnnotation"`
		} `xml:"Element"`
	} `xml:"Model"`
}

type ColumnException struct {
	SchemaName    string `json:"schemaName"`
	TableName     string `json:"tableName"`
	ColName       string `json:"columnName"`
	DataType      int
	DtStr         string `json:"dataType"`
	Length        int    `json:"length"`
	Scale         int    `json:"scale"`
	Precision     int    `json:"precision"`
	IsNullable    bool   `json:"isNullable"`
	IsAdulterated bool   `json:"isAdulterated"`
}

type ColumnExceptions struct {
	Columns []ColumnException
}

// TableColumn struct contains the definition for an exported database column
type TableColumn struct {
	ColName       string
	DataType      int
	DtStr         string
	Length        int
	Scale         int
	Precision     int
	IsNullable    bool
	IsAdulterated bool // flag to indicate if the byte-stream for the column is supected of having been messed with
}

// Table struct contains the definition for an exported database table
type Table struct {
	dataDir string
	Schema  string
	TabName string
	Columns []TableColumn
}

// UserDefinedType struct contains the definition for an exported user
// defined type. This is used for mapping table columns of type <user
// defined type> to the underlying base data type
type UserDefinedType struct {
	Schema     string
	Name       string
	DataType   int
	DtStr      string
	Length     int
	Scale      int
	Precision  int
	IsNullable bool
}

// ExtractedModel contains the model data needed for identifying, and
// extracting the data from, all the exported tables
type ExtractedModel struct {
	baseDir                string
	Collation              string
	CollationCaseSensitive bool
	FileFormatVersion      string
	SchemaVersion          string
	DspName                string
	Tables                 map[string]Table
}

// dtMap maps the datatype strings in the model.xml file to the appropriate datatype enums
var dtMap = map[string]int{
	"bigint":           BigInt,
	"binary":           Binary,
	"bit":              Bit,
	"char":             Char,
	"datetime":         Datetime,
	"datetime2":        Datetime2,
	"decimal":          Decimal,
	"float":            Float,
	"geography":        Geography,
	"int":              Int,
	"nchar":            NChar,
	"ntext":            NText,
	"numeric":          Numeric,
	"nvarchar":         NVarchar,
	"real":             Real,
	"smalldatetime":    SmallDatetime,
	"smallint":         SmallInt,
	"smallmoney":       SmallMoney,
	"sql_variant":      SQLVariant,
	"text":             Text,
	"tinyint":          TinyInt,
	"uniqueidentifier": UniqueIdentifier,
	"varbinary":        Varbinary,
	"varchar":          Varchar,
}

// ModelFileName returns the path/name for the model xml file
func (bp Bacpac) ModelFileName() (n string) {
	n = catDir([]string{bp.baseDir, "model.xml"})
	return n
}

// GetModel extracts the portions of the table definitions needed for
// properly parsing/extracting the data from the BCP data files.
func (bp Bacpac) GetModel(ef string) (m ExtractedModel, err error) {

	m.baseDir = bp.baseDir

	f, err := os.Open(bp.ModelFileName())
	if err != nil {
		return m, err
	}
	defer func() {
		if cerr := f.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	dec := xml.NewDecoder(f)
	dec.CharsetReader = charset.NewReaderLabel
	dec.Strict = false

	var doc DataSchemaModel
	if err = dec.Decode(&doc); err != nil {
		return m, err
	}

	m.Collation = doc.CollationLcid
	m.FileFormatVersion = doc.FileFormatVersion
	m.SchemaVersion = doc.SchemaVersion
	m.DspName = doc.DspName
	m.CollationCaseSensitive = doc.CollationCaseSensitive == "True"

	// Grab the custom data types: name, schema, base type, length
	types := extractUserTypes(doc)

	var exceptions ColumnExceptions
	if ef != "" {
		var data []byte
		data, err = ioutil.ReadFile(ef)
		if err != nil {
			return m, err
		}

		err = json.Unmarshal(data, &exceptions)
		if err != nil {
			return m, err
		}
	}

	// Grab the table definition data, using the custom data types to
	// translate to base types -- don't know if composite types are
	// possible but if they are, I don't have any to test with anyhow...
	rt := bp.getTables(doc, types, exceptions)

	m.Tables = rt

	return m, err
}

// getTables extracts the table definitions from the schema model
func (bp Bacpac) getTables(doc DataSchemaModel, userTypes map[string]UserDefinedType, exceptions ColumnExceptions) (rt map[string]Table) {

	rt = make(map[string]Table)

	var ex map[string]ColumnException
	ex = make(map[string]ColumnException)
	for _, v := range exceptions.Columns {
		k := strings.Join([]string{v.SchemaName, v.TableName, v.ColName}, ".")
		dt, ok := dtMap[v.DtStr]
		if ok {
			v.DataType = dt
		}
		ex[k] = v
	}

	for _, element := range doc.Model.Element {
		if element.Type != "SqlTable" {
			continue
		}

		var t Table

		tokens := strings.Split(element.Name, ".")
		t.Schema = strings.Trim(tokens[0], "[]")
		t.TabName = strings.Trim(tokens[1], "[]")

		dd := strings.Join([]string{t.Schema, t.TabName}, ".")
		t.dataDir = catDir([]string{bp.baseDir, "Data", dd})

		for _, relationship := range element.Relationship {
			if relationship.Name != "Columns" {
				continue
			}
			for _, entry := range relationship.Entry {
				if entry.Element.Type != "SqlSimpleColumn" {
					continue
				}

				var col TableColumn
				tokens := strings.Split(entry.Element.Name, ".")
				col.ColName = strings.Trim(tokens[2], "[]")
				col.IsNullable = true

				for _, re := range entry.Element.Relationship.Entry {
					if re.Element.Type != "SqlTypeSpecifier" {
						continue
					}

					// Determine the column properties... If the column
					// datatype is a user defined datatype then default
					// to the values defined for the user defined datatype
					n := re.Element.Relationship.Entry.References.Name
					col.DtStr = strings.Replace(strings.Trim(n, "[]"), "].[", ".", -1)

					ut, ok := userTypes[col.DtStr]
					if ok {
						col.DtStr = ut.DtStr
						col.DataType = ut.DataType
						col.Length = ut.Length
						col.Scale = ut.Scale
						col.IsNullable = ut.IsNullable
					} else {
						col.DataType = dtMap[col.DtStr]
					}

					for _, p := range re.Element.Property {
						if p.Name == "Length" {
							col.Length, _ = toInt([]byte(p.Value))
						} else if p.Name == "Scale" {
							col.Scale, _ = toInt([]byte(p.Value))
						} else if p.Name == "Precision" {
							col.Precision, _ = toInt([]byte(p.Value))
						} else if p.Name == "IsNullable" && p.Value == "False" {
							col.IsNullable = false
						}
					}
				}

				for _, p := range entry.Element.Property {
					if p.Name == "Length" {
						col.Length, _ = toInt([]byte(p.AttrValue))
					} else if p.Name == "Scale" {
						col.Scale, _ = toInt([]byte(p.AttrValue))
					} else if p.Name == "Precision" {
						col.Precision, _ = toInt([]byte(p.AttrValue))
					} else if p.Name == "IsNullable" && p.AttrValue == "False" {
						col.IsNullable = false
					}
				}

				// Check for column definition exceptions
				k := strings.Join([]string{t.Schema, t.TabName, col.ColName}, ".")
				v, ok := ex[k]
				if ok {
					col.DtStr = v.DtStr
					col.DataType = v.DataType
					col.Length = v.Length
					col.Scale = v.Scale
					col.Precision = v.Precision
					col.IsNullable = v.IsNullable
					col.IsAdulterated = v.IsAdulterated
				}

				t.Columns = append(t.Columns, col)
			}
		}
		key := strings.Join([]string{t.Schema, t.TabName}, ".")
		rt[key] = t
	}

	return rt
}

// extractUserTypes extracts the user defined types from the schema model
func extractUserTypes(doc DataSchemaModel) (rt map[string]UserDefinedType) {

	// <Model>
	// ...
	//     <Element Type="SqlUserDefinedDataType" Name="[dbo].[custom_field]">
	//         <Property Name="Length" Value="255" />
	//         <Relationship Name="Schema">
	//             <Entry>
	//                 <References ExternalSource="BuiltIns" Name="[dbo]" />
	//             </Entry>
	//         </Relationship>
	//         <Relationship Name="Type">
	//             <Entry>
	//                 <References ExternalSource="BuiltIns" Name="[varchar]" />
	//             </Entry>
	//         </Relationship>
	//     </Element>
	// ...

	rt = make(map[string]UserDefinedType)

	for _, element := range doc.Model.Element {
		if element.Type != "SqlUserDefinedDataType" {
			continue
		}

		var t UserDefinedType
		t.Name = strings.Replace(strings.Trim(element.Name, "[]"), "].[", ".", -1)
		t.IsNullable = true

		for _, p := range element.Property {
			if p.Name == "Length" {
				len, _ := toInt([]byte(p.AttrValue))
				t.Length = len
			} else if p.Name == "Precision" {
				t.Precision, _ = toInt([]byte(p.AttrValue))
			} else if p.Name == "Scale" {
				t.Scale, _ = toInt([]byte(p.AttrValue))
			} else if p.Name == "IsNullable" && p.AttrValue == "False" {
				t.IsNullable = false
			}
		}

		for _, r := range element.Relationship {
			for _, entry := range r.Entry {
				if r.Name == "Schema" {
					t.Schema = strings.Trim(entry.References.Name, "[]")
				} else if r.Name == "Type" {
					t.DtStr = strings.Replace(strings.Trim(entry.References.Name, "[]"), "].[", ".", -1)
					t.DataType = dtMap[t.DtStr]
				}
			}
		}
		rt[t.Name] = t
	}
	return rt
}
