package context

type Schema struct {
	Dataset     string
	TableName   string
	ColumnName  string
	DataType    string
	Ordinal     int
	Description string
}

type Lineage struct {
	SourceAsset   string
	TargetAsset   string
	SourceDataset string
	SourceTable   string
	TargetDataset string
	TargetTable   string
}

type Asset struct {
	AssetName     string
	Dataset       string
	TableName     string
	Layer         string
	Grain         string
	Docstring     string
	DirectiveJSON string
}
