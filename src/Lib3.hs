{-# LANGUAGE DeriveFunctor #-}

module Lib3
  ( executeSql,
    parseTables,
    Execution,
    ExecutionAlgebra(..),
    ParsedStatement(..),
    SelectColumn(..),
    TableName,
    loadFiles,
    getTime,
    parseYAMLContent,
    getTableDfByName,
    dataFrameToSerializedTable,
    serializeTableToYAML,
  )
where

import Control.Monad.Free (Free (..), liftF)

import DataFrame (Column(..), ColumnType(..), Value(..), Row, DataFrame(..))
import Data.Yaml (decodeEither')
import Data.Text.Encoding as TE
import Data.Text as T
import Data.List
import qualified Data.Yaml as Y
import Data.Char (toLower)
import Data.Time (UTCTime)
import qualified Data.Aeson.Key as Key
import qualified Data.ByteString.Char8 as BS

type TableName = String
type FileContent = String
type ErrorMessage = String
type SQLQuery = String
type ColumnName = String

data SerializedTable = SerializedTable
  { tableName :: TableName
  , columns   :: [SerializedColumn]
  , rows      :: [[Y.Value]]
  }

data SerializedColumn = SerializedColumn
  { name     :: String
  , dataType :: String
  }

instance Y.FromJSON SerializedTable where
  parseJSON = Y.withObject "SerializedTable" $ \v -> SerializedTable
    <$> v Y..: Key.fromString "tableName"
    <*> v Y..: Key.fromString "columns"
    <*> v Y..: Key.fromString "rows"

instance Y.FromJSON SerializedColumn where
  parseJSON = Y.withObject "SerializedColumn" $ \v -> SerializedColumn
    <$> v Y..: Key.fromString "name"
    <*> v Y..: Key.fromString "dataType"
  
data SelectColumn
  = Now
  | TableColumn TableName ColumnName
  deriving (Show, Eq)

type SelectedColumns = [SelectColumn]
type SelectedTables = [TableName]

data ParsedStatement
  = SelectAll SelectedTables (Maybe WhereClause)
  | SelectAggregate SelectedTables SelectedColumns (Maybe WhereClause)
  | SelectColumns SelectedTables SelectedColumns (Maybe WhereClause)
  | DeleteStatement TableName (Maybe WhereClause)
  | InsertStatement TableName SelectedColumns Row
  | UpdateStatement TableName SelectedColumns Row (Maybe WhereClause)
  | Invalid ErrorMessage
  deriving (Show, Eq)

data WhereClause
  = IsValueBool Bool TableName String
  | Conditions [Condition]
  deriving (Show, Eq)

data Condition
  = Equals SelectColumn ConditionValue
  | GreaterThan SelectColumn ConditionValue
  | LessThan SelectColumn ConditionValue
  | LessthanOrEqual SelectColumn ConditionValue
  | GreaterThanOrEqual SelectColumn ConditionValue
  | NotEqual SelectColumn ConditionValue
  deriving (Show, Eq)

data ConditionValue
  = StrValue String
  | IntValue Integer
  deriving (Show, Eq)

data StatementType = Select | Delete | Insert | Update | ShowTable | ShowTables | InvalidStatement

data ExecutionAlgebra next
  = LoadFiles [TableName] ([FileContent] -> next)
  | ParseTables [FileContent] ([(TableName, DataFrame)] -> next)
  | GetTableDfByName TableName [(TableName, DataFrame)] (DataFrame -> next)
  | UpdateTable (TableName, DataFrame) next
  | GetTime (UTCTime -> next)
  | GetStatementType SQLQuery (StatementType -> next)
  | ParseSql SQLQuery (ParsedStatement -> next)
  | IsParsedStatementValid ParsedStatement [(TableName, DataFrame)] (Bool -> next)
  | GetTableNames ParsedStatement ([TableName] -> next)
  | DeleteRows ParsedStatement [(TableName, DataFrame)] ((TableName, DataFrame) -> next)
  | InsertRows ParsedStatement [(TableName, DataFrame)] ((TableName, DataFrame) -> next)
  | UpdateRows ParsedStatement [(TableName, DataFrame)] ((TableName, DataFrame) -> next)
  | GetSelectedColumns ParsedStatement [(TableName, DataFrame)] ([Column] -> next)
  | GetReturnTableRows ParsedStatement [(TableName, DataFrame)] UTCTime ([Row] -> next)
  | GenerateDataFrame [Column] [Row] (DataFrame -> next)
  | ShowTablesFunction [TableName] (DataFrame -> next)
  | ShowTableFunction DataFrame (DataFrame -> next)
  | GetNotSelectTableName ParsedStatement (TableName -> next)
  deriving Functor

type Execution = Free ExecutionAlgebra

loadFiles :: [TableName] -> Execution [FileContent]
loadFiles names = liftF $ LoadFiles names id

parseTables :: [FileContent] -> Execution [(TableName, DataFrame)]
parseTables content = liftF $ ParseTables content id

getTableDfByName :: TableName -> [(TableName, DataFrame)] -> Execution DataFrame
getTableDfByName tableName tables = liftF $ GetTableDfByName tableName tables id

updateTable :: (TableName, DataFrame) -> Execution ()
updateTable table = liftF $ UpdateTable table ()

getTime :: Execution UTCTime
getTime = liftF $ GetTime id

getStatementType :: SQLQuery -> Execution StatementType
getStatementType query = liftF $ GetStatementType query id

parseSql :: SQLQuery -> Execution ParsedStatement
parseSql query = liftF $ ParseSql query id

isParsedStatementValid :: ParsedStatement -> [(TableName, DataFrame)] -> Execution Bool
isParsedStatementValid statement tables = liftF $ IsParsedStatementValid statement tables id

getTableNames :: ParsedStatement -> Execution [TableName]
getTableNames statement = liftF $ GetTableNames statement id

deleteRows :: ParsedStatement -> [(TableName, DataFrame)] -> Execution (TableName, DataFrame)
deleteRows statement tables = liftF $ DeleteRows statement tables id

insertRows :: ParsedStatement -> [(TableName, DataFrame)] -> Execution (TableName, DataFrame)
insertRows statement tables = liftF $ InsertRows statement tables id

updateRows :: ParsedStatement -> [(TableName, DataFrame)] -> Execution (TableName, DataFrame)
updateRows statement tables = liftF $ UpdateRows statement tables id

getSelectedColumns :: ParsedStatement -> [(TableName, DataFrame)] -> Execution [Column]
getSelectedColumns statement tables = liftF $ GetSelectedColumns statement tables id

getReturnTableRows :: ParsedStatement -> [(TableName, DataFrame)] -> UTCTime -> Execution [Row]
getReturnTableRows parsedStatement usedTables time = liftF $ GetReturnTableRows parsedStatement usedTables time id

generateDataFrame :: [Column] -> [Row] -> Execution DataFrame
generateDataFrame columns rows = liftF $ GenerateDataFrame columns rows id

showTablesFunction :: [TableName] -> Execution DataFrame
showTablesFunction tables = liftF $ ShowTablesFunction tables id

showTableFunction :: DataFrame -> Execution DataFrame
showTableFunction table = liftF $ ShowTableFunction table id

getNonSelectTableName :: ParsedStatement -> Execution TableName
getNonSelectTableName statement = liftF $ GetNotSelectTableName statement id

executeSql :: SQLQuery -> Execution (Either ErrorMessage DataFrame)
executeSql statement = do
  parsedStatement <- parseSql statement

  tableNames <- getTableNames parsedStatement
  tableFiles <- loadFiles tableNames
  tables <- parseTables tableFiles
  statementType <- getStatementType statement
  timeStamp     <- getTime
  isValid <- isParsedStatementValid parsedStatement tables

  if not isValid
    then return $ Left "err"
    else
    case statementType of
      Select -> do
        columns     <- getSelectedColumns parsedStatement tables
        rows        <- getReturnTableRows parsedStatement tables timeStamp
        df          <- generateDataFrame columns rows
        return $ Right df
      Delete -> do
        (name, df)  <- deleteRows parsedStatement tables
        updateTable (name, df)
        return $ Right df
      Insert -> do
        (name, df)  <- insertRows parsedStatement tables
        updateTable (name, df)
        return $ Right df
      Update -> do
        (name, df)  <- updateRows parsedStatement tables
        updateTable (name, df)
        return $ Right df
      ShowTables -> do
        allTables   <- showTablesFunction tableNames
        return $ Right allTables
      ShowTable -> do
        nonSelectTableName <- getNonSelectTableName parsedStatement
        df          <- getTableDfByName nonSelectTableName tables
        allTables   <- showTableFunction df
        return $ Right allTables
      InvalidStatement -> do
        return $ Left "Invalid statement"


parseYAMLContent :: FileContent -> Either ErrorMessage (TableName, DataFrame)
parseYAMLContent content = 
  case decodeEither' (BS.pack content) of
    Left err -> Left $ "YAML parsing error: " ++ show err
    Right serializedTable -> Right $ convertToDataFrame serializedTable

convertToDataFrame :: SerializedTable -> (TableName, DataFrame)
convertToDataFrame st = (tableName st, DataFrame (Prelude.map convertColumn $ columns st) (convertRows $ rows st))

convertColumn :: SerializedColumn -> Column
convertColumn sc = Column (name sc) (convertColumnType $ dataType sc)

convertColumnType :: String -> ColumnType
convertColumnType dt = case dt of
    "integer" -> IntegerType
    "string" -> StringType
    "boolean" -> BoolType
    _ -> error "Unknown column type"

convertRows :: [[Y.Value]] -> [Row]
convertRows = Prelude.map (Prelude.map convertValue)

convertValue :: Y.Value -> DataFrame.Value
convertValue val = case val of
    Y.String s -> DataFrame.StringValue (T.unpack s)
    Y.Number n -> DataFrame.IntegerValue (round n) 
    Y.Bool b -> DataFrame.BoolValue b
    Y.Null -> DataFrame.NullValue
    _ -> error "Unsupported value type"

serializeTableToYAML :: SerializedTable -> String
serializeTableToYAML st =
    "tableName: " ++ tableName st ++ "\n" ++
    "columns:\n" ++ Prelude.concatMap serializeColumn (columns st) ++
    "rows:\n" ++ Prelude.concatMap serializeRow (rows st)
  where
    serializeColumn :: SerializedColumn -> String
    serializeColumn col =
        "- name: " ++ name col ++ "\n" ++
        "  dataType: " ++ dataType col ++ "\n"

    serializeRow :: [Y.Value] -> String
    serializeRow row = "- [" ++ Data.List.intercalate ", " (Prelude.map serializeValue row) ++ "]\n"

    serializeValue :: Y.Value -> String
    serializeValue val =
        case val of
            Y.String s -> T.unpack s 
            Y.Number n -> show (round n :: Int)
            Y.Bool b   -> Prelude.map Data.Char.toLower (show b)
            Y.Null     -> "null"

dataFrameToSerializedTable :: (TableName, DataFrame) -> SerializedTable
dataFrameToSerializedTable (tblName, DataFrame columns rows) =
    SerializedTable {
        tableName = tblName,
        columns = Prelude.map convertColumn columns,
        rows = Prelude.map convertRow rows
    }
  where
    convertColumn :: Column -> SerializedColumn
    convertColumn (Column name columnType) =
        SerializedColumn {
            name = name,
            dataType = convertColumnType columnType
        }

    convertColumnType :: ColumnType -> String
    convertColumnType IntegerType = "integer"
    convertColumnType StringType = "string"
    convertColumnType BoolType = "boolean"

    convertRow :: Row -> [Y.Value]
    convertRow = Prelude.map convertValue

    convertValue :: Value -> Y.Value
    convertValue (IntegerValue n) = Y.Number (fromIntegral n)
    convertValue (StringValue s) = Y.String (T.pack s)
    convertValue (BoolValue b) = Y.Bool b
    convertValue NullValue = Y.Null