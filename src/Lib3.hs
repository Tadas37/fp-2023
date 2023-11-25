{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE RankNTypes #-}

module Lib3
  ( executeSql,
    parseTables,
    Execution,
    ExecutionAlgebra(..),
    ParsedStatement(..),
    WhereClause(..),
    Condition(..),
    ConditionValue(..),
    loadFiles,
    getTime,
    parseYAMLContent,
    getTableDfByName,
    dataFrameToSerializedTable,
    serializeTableToYAML,
    filterRows,
    filterDataFrame,
    rowSatisfiesWhereClause,
    conditionSatisfied,
    compareWithCondition,
    compareValue,
    findColumnIndex,
    createRowFromValues,
    extractColumnNames,
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
import Data.Maybe
import Text.Read (readMaybe)

type TableName = String
type FileContent = String
type ErrorMessage = String
type SQLQuery = String
type ColumnName = String
type InsertValue = String

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
type SelectedValues = [InsertValue]

data ParsedStatement
  = SelectAll SelectedTables (Maybe WhereClause)
  | SelectAggregate SelectedTables SelectedColumns (Maybe WhereClause)
  | SelectColumns SelectedTables SelectedColumns (Maybe WhereClause)
  | DeleteStatement TableName (Maybe WhereClause)
  | InsertStatement TableName (Maybe SelectedColumns) SelectedValues
  | UpdateStatement TableName SelectedColumns Row (Maybe WhereClause)
  | Invalid ErrorMessage
  deriving (Show, Eq)

data WhereClause
  = IsValueBool Bool TableName String
  | Conditions [Condition]
  deriving (Show, Eq)

data Condition
  = Equals String ConditionValue
  | GreaterThan String ConditionValue
  | LessThan String ConditionValue
  | LessthanOrEqual String ConditionValue
  | GreaterThanOrEqual String ConditionValue
  | NotEqual String ConditionValue
  deriving (Show, Eq)

data ConditionValue
  = StrValue String
  | IntValue Integer
  deriving (Show, Eq)

data StatementType = Select | Delete | Insert | Update | ShowTable | ShowTables

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

filterRows :: DataFrame -> Maybe WhereClause -> Either String DataFrame
filterRows df@(DataFrame cols _) (Just wc) =
    if whereClauseHasValidColumns wc cols
    then filterDataFrame df wc
    else Left "Error: Specified column in WhereClause does not exist."
filterRows (DataFrame cols rows) Nothing = Right $ DataFrame cols []

whereClauseHasValidColumns :: WhereClause -> [Column] -> Bool
whereClauseHasValidColumns (IsValueBool _ _ columnName) cols = isJust (findColumnIndex columnName cols)
whereClauseHasValidColumns (Conditions conditions) cols = Data.List.all (`conditionHasValidColumn` cols) conditions

conditionHasValidColumn :: Condition -> [Column] -> Bool
conditionHasValidColumn condition cols = isJust (findColumnIndex (columnNameFromCondition condition) cols)

columnNameFromCondition :: Condition -> String
columnNameFromCondition (Equals colName _) = colName
columnNameFromCondition (GreaterThan colName _) = colName
columnNameFromCondition (LessThan colName _) = colName
columnNameFromCondition (LessthanOrEqual colName _) = colName
columnNameFromCondition (GreaterThanOrEqual colName _) = colName
columnNameFromCondition (NotEqual colName _) = colName

filterDataFrame :: DataFrame -> WhereClause -> Either String DataFrame
filterDataFrame (DataFrame cols rows) wc =
    let filteredRows = Data.List.filter (rowSatisfiesWhereClause wc cols) rows
    in if Data.List.null filteredRows
       then Left "Error: No rows match the specified condition."
       else Right $ DataFrame cols filteredRows

rowSatisfiesWhereClause :: WhereClause -> [Column] -> Row -> Bool
rowSatisfiesWhereClause (IsValueBool b _ columnName) cols row =
    case findColumnIndex columnName cols of
        Just idx -> case row !! idx of
            BoolValue val -> val == b
            _ -> False
        Nothing -> False
rowSatisfiesWhereClause (Conditions conditions) cols row =
    Data.List.all (`conditionSatisfied` (cols, row)) conditions

conditionSatisfied :: Condition -> ([Column], Row) -> Bool
conditionSatisfied (Equals colName condValue) (cols, row) =
    compareWithCondition colName cols row (==) condValue
conditionSatisfied (GreaterThan colName condValue) (cols, row) =
    compareWithCondition colName cols row (>) condValue
conditionSatisfied (LessThan colName condValue) (cols, row) =
    compareWithCondition colName cols row (<) condValue
conditionSatisfied (LessthanOrEqual colName condValue) (cols, row) =
    compareWithCondition colName cols row (<=) condValue
conditionSatisfied (GreaterThanOrEqual colName condValue) (cols, row) =
    compareWithCondition colName cols row (>=) condValue
conditionSatisfied (NotEqual colName condValue) (cols, row) =
    compareWithCondition colName cols row (/=) condValue

compareWithCondition :: String -> [Column] -> Row -> (forall a. Ord a => a -> a -> Bool) -> ConditionValue -> Bool
compareWithCondition colName cols row op condValue =
    case findColumnIndex colName cols of
        Just idx -> compareValue (row !! idx) op condValue
        Nothing -> False

compareValue :: Value -> (forall a. Ord a => a -> a -> Bool) -> ConditionValue -> Bool
compareValue (IntegerValue i) op (IntValue v) = i `op` v
compareValue (StringValue s) op (StrValue v) = s `op` v
compareValue _ _ _ = False

findColumnIndex :: String -> [Column] -> Maybe Int
findColumnIndex columnName cols = Data.List.findIndex (\(Column name _) -> name == columnName) cols

createRowFromValues :: Maybe [ColumnName] -> [Column] -> [InsertValue] -> Either String Row
createRowFromValues maybeSelectedColumns cols values =
    case maybeSelectedColumns of
        Just selectedColumns -> 
            if Data.List.length selectedColumns == Data.List.length values then
                sequence $ Data.List.zipWith (matchValueToColumn cols) selectedColumns values
            else
                Left "Error: Number of specified columns and values does not match."
        Nothing -> 
            if Data.List.length cols == Data.List.length values then
                sequence $ Data.List.zipWith matchValueToColumnType cols values
            else
                Left "Error: Number of table columns and values does not match."

matchValueToColumn :: [Column] -> ColumnName -> InsertValue -> Either String Value
matchValueToColumn cols colName value =
    case Data.List.find (\(Column name _) -> name == colName) cols of
        Just col -> matchValueToColumnType col value
        Nothing  -> Left $ "Error: Column " ++ colName ++ " does not exist."

matchValueToColumnType :: Column -> InsertValue -> Either String Value
matchValueToColumnType (Column _ IntegerType) value =
    maybe (Left "Error: Invalid integer value.") (Right . IntegerValue) (readMaybe value :: Maybe Integer)
matchValueToColumnType (Column _ StringType) value =
    Right $ StringValue value
matchValueToColumnType (Column _ BoolType) value =
    case Data.List.map Data.Char.toLower value of
        "true"  -> Right $ BoolValue True
        "false" -> Right $ BoolValue False
        _       -> Left "Error: Invalid boolean value."

extractColumnNames :: SelectedColumns -> [ColumnName]
extractColumnNames = mapMaybe extractName
  where
    extractName :: SelectColumn -> Maybe ColumnName
    extractName (TableColumn _ colName) = Just colName
    extractName _ = Nothing
