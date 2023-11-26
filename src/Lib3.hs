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
    SelectColumn(..),
    TableName,
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
    validateStatement,
    SerializedTable(..),
    getSelectedColumnsFunction,
    updateRowsInTable,
    parseStatement,
    ParsedStatement (..)
  )
where

import Control.Monad.Free (Free (..), liftF)

import DataFrame (Column(..), ColumnType(..), Value(..), Row, DataFrame(..))
import Data.Yaml (decodeEither')
import Data.Text.Encoding as TE
import Data.Text as T
import Data.List
import Data.Maybe (mapMaybe)
import Data.List
    ( map,
      elem,
      length,
      null,
      all,
      words,
      break,
      drop,
      dropWhile,
      head,
      init,
      last,
      intercalate,
      isPrefixOf,
      isSuffixOf)
import qualified Data.Yaml as Y
import Data.Char (toLower, isDigit)
import Data.Time (UTCTime)
import qualified Data.Aeson.Key as Key
import qualified Data.ByteString.Char8 as BS
import Data.Maybe
import Text.Read (readMaybe)
import Data.Maybe (isJust, isNothing)

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
  | Max TableName ColumnName
  | Avg TableName ColumnName
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
  | ShowTableStatement TableName
  | ShowTablesStatement
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

data SelectType = Aggregate | ColumnsAndTime | AllColumns

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
isParsedStatementValid statement tables = liftF $ IsParsedStatementValid statement tables (\_ -> validateStatement statement tables)

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

updateRowsInTable :: ParsedStatement -> DataFrame -> DataFrame
updateRowsInTable (UpdateStatement _ _ newRow maybeWhereClause) (DataFrame columns rows) =
   DataFrame columns (Data.List.map (updateRowIfMatches newRow maybeWhereClause) rows)
updateRowsInTable _ df = df

updateRowIfMatches :: Row -> Maybe WhereClause -> Row -> Row
updateRowIfMatches newRow (Just whereClause) row =
    if rowMatchesWhereClause row whereClause
    then newRow
    else row
updateRowIfMatches newRow Nothing _ = newRow

rowMatchesWhereClause :: Row -> WhereClause -> Bool
rowMatchesWhereClause row (IsValueBool b tableName columnName) = False
rowMatchesWhereClause row (Conditions conditions) = Data.List.all (rowMatchesCondition row) conditions

rowMatchesCondition :: Row -> Condition -> Bool
rowMatchesCondition row (Equals (TableColumn _ columnName) (IntValue value)) = False

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
    
validateStatement :: ParsedStatement -> [(TableName, DataFrame)] -> Bool
validateStatement stmt tables = case stmt of
  SelectAll tableNames _ -> Data.List.all (`Data.List.elem` Data.List.map fst tables) tableNames
  SelectColumns tableNames cols _ -> validateTableAndColumns tableNames cols tables
  InsertStatement tableName cols _ -> validateTableAndColumns [tableName] cols tables
  UpdateStatement tableName cols _ _ -> validateTableAndColumns [tableName] cols tables
  DeleteStatement tableName _ -> tableName `Data.List.elem` Data.List.map fst tables
  _ -> True  

validateTableAndColumns :: [TableName] -> [SelectColumn] -> [(TableName, DataFrame)] -> Bool
validateTableAndColumns tableNames cols tables = Data.List.all tableAndColumnsExist tableNames
  where
    tableAndColumnsExist tableName = maybe False (columnsExistInTable cols) (lookup tableName tables)

    columnsExistInTable :: [SelectColumn] -> DataFrame -> Bool
    columnsExistInTable columns df = Data.List.all (`columnExistsInDataFrame` df) columns

    columnExistsInDataFrame :: SelectColumn -> DataFrame -> Bool
    columnExistsInDataFrame (TableColumn _ colName) (DataFrame cols _) = 
      Data.List.any (\(Column name _) -> name == colName) cols
    columnExistsInDataFrame _ _ = True

getSelectedColumnsFunction :: Lib3.ParsedStatement -> [(Lib3.TableName, DataFrame)] -> [Column]
getSelectedColumnsFunction stmt tbls = case stmt of
    Lib3.SelectAll tableNames _ -> Data.List.concatMap (getTableColumns tbls) tableNames
    Lib3.SelectColumns _ selectedColumns _ -> mapMaybe (findColumn tbls) selectedColumns
    _ -> []

getTableColumns :: [(Lib3.TableName, DataFrame)] -> Lib3.TableName -> [Column]
getTableColumns tbls tableName = case lookup tableName tbls of
    Just (DataFrame columns _) -> columns
    Nothing -> []

findColumn :: [(Lib3.TableName, DataFrame)] -> Lib3.SelectColumn -> Maybe Column
findColumn tbls (Lib3.TableColumn tblName colName) =
    case lookup tblName tbls of
        Just (DataFrame columns _) -> Data.List.find (\(Column name _) -> name == colName) columns
        Nothing -> Nothing

-- ONLY SQL PARSER BELOW

parseStatement :: String  -> Either ErrorMessage ParsedStatement
parseStatement input
  | Data.List.last input /= ';' = Left "Missing semicolon at end of statement"
  | otherwise = mapStatementType wordsInput
  where
    cleanedInput = Data.List.init input
    wordsInput = parseSemiCaseSensitive cleanedInput

mapStatementType :: [String] -> Either ErrorMessage ParsedStatement
mapStatementType statement = do
  statementType <- guessStatementType statement
  case statementType of
      Select -> do
        parseSelect (Data.List.drop 1 statement)
      Insert -> do
        parseInsert statement
      Delete -> do
        parseDelete statement
      Update -> do
        parseUpdate statement
      ShowTable -> do
        parseShowTable statement
      ShowTables -> do
        parseShowTables statement
      InvalidStatement -> do
        Left "Invalid statement"

guessStatementType :: [String] -> Either ErrorMessage StatementType
guessStatementType (x : y : _)
  | x == "select" = Right Select
  | x == "insert" && y == "into" = Right Insert
  | x == "delete" && y == "from" = Right Delete
  | x == "update" = Right Update
  | x == "show" && y == "table" = Right ShowTable
  | x == "show" && y == "tables" = Right ShowTables
  | otherwise = Left "Sql statement does not resemble any sort of implemented statement"

guessStatementType _ = Left "Sql statement does not resemble any sort of implemented statement"

parseInsert :: [String] -> Either ErrorMessage ParsedStatement
parseInsert statement = do
  tableName <- getInsertTableName statement
  getInsertColumns statement tableName

getInsertTableName :: [String] -> Either ErrorMessage TableName
getInsertTableName statement = do
  (_, intoAndAfter) <- Right $ Data.List.break (== "into") statement
  tableToInsertInto <- Right $ Data.List.drop 1 intoAndAfter
  if not (Data.List.null tableToInsertInto) && isValidTable (Data.List.head tableToInsertInto)
      then Right $ Data.List.head tableToInsertInto
      else Left "Update statement table name does not meet requirements. Maybe ilegal characters were used?"

getInsertColumns :: [String] -> TableName -> Either ErrorMessage ParsedStatement
getInsertColumns statement tableName = do
  (_, intoAndAfter) <- Right $ Data.List.break (== "into") statement
  (intoToValues, valuesAndAfter) <- Right $ Data.List.break (== "values") intoAndAfter
  cleanedValues <- cleanInsertHeadAndTail (Data.List.drop 1 valuesAndAfter)
  columnNames <- getColumnNames $ Data.List.drop 2 intoToValues
  columnValues <- getInsertColumnValues cleanedValues
  columns <-
    if Data.List.length columnNames /= Data.List.length columnValues
      then Left "Column count does not match value count in insert statement"
      else Right $ Data.List.map (TableColumn tableName) columnNames
  getInsertStatement tableName columns columnValues

getInsertStatement :: TableName -> [SelectColumn] -> [Value] -> Either ErrorMessage ParsedStatement
getInsertStatement tableName columns values = Right $ InsertStatement tableName columns values

getInsertColumnValues :: [String] -> Either ErrorMessage [Value]
getInsertColumnValues [value] = do
  val <- getValueFromString value
  return [val]

getInsertColumnValues (value : xs) = do
  cleanedString <- if Data.List.last value == ',' then Right $ Data.List.init value else Left "Failed to parse INSERT values"
  val <- getValueFromString cleanedString
  rest <- getInsertColumnValues xs
  return $ rest ++ [val]

getInsertColumnValues _ = Left "Error parsing insert statement values"

getColumnNames :: [String] -> Either ErrorMessage [String]
getColumnNames rawNames = do
  cleanedHeadAndTail <- cleanInsertHeadAndTail rawNames
  cleanInsertCommas cleanedHeadAndTail

cleanInsertHeadAndTail :: [String] -> Either ErrorMessage [String]
cleanInsertHeadAndTail input =
  if not (Data.List.null input) && Data.List.head (Data.List.head input) == '(' && Data.List.last (Data.List.last input) == ')'
    then
      if Data.List.length input == 1
        then Right [Data.List.drop 1 (Data.List.init (Data.List.head input))]
        else Right $ [Data.List.drop 1 (Data.List.head input)] ++ Data.List.init (Data.List.drop 1 input) ++ [Data.List.init (Data.List.last input)]
    else Left "formating of insert statement does not meet requirements. Most likely issing `(` or `)`"

cleanInsertCommas :: [String] -> Either ErrorMessage [String]
cleanInsertCommas [column] =
  if isValidColumnWithoutAb column
    then Right [column]
    else Left $ "Column" ++ " `" ++ column ++ "` " ++ "contains ilegal characters"

cleanInsertCommas (column : xs) = do
  currentColumn <-
    if Data.List.last column == ',' && isValidColumnWithoutAb column
      then Right $ Data.List.init column
      else Left "Missing comma or invalid column name for insert statement"
  rest <- cleanInsertCommas xs
  return $ rest ++ [currentColumn]

cleanInsertCommas _ = Left "Unknown error parsing columns in insert statement"

parseDelete :: [String] -> Either ErrorMessage ParsedStatement
parseDelete statement = do
  tableName <- getTableNameFromDelete statement
  (_, fromWhere) <- Right $ Data.List.break (== "where") statement
  whereClause <- statementClause' fromWhere tableName
  getDeleteStatement tableName whereClause

getDeleteStatement :: TableName -> Maybe WhereClause -> Either ErrorMessage ParsedStatement
getDeleteStatement tableName whereClause = Right $ DeleteStatement tableName whereClause

getTableNameFromDelete :: [String] -> Either ErrorMessage TableName
getTableNameFromDelete statement = do
  (_, fromAndElse) <- Right $ Data.List.break (== "from") statement
  (fromToWhere, _) <- Right $ Data.List.break (== "where") fromAndElse
  if Data.List.length fromToWhere /= 2 || not (isValidTable (Data.List.last fromToWhere))
      then Left $ "Invalid delete statement table name: " ++ Data.List.last fromToWhere
      else Right $ Data.List.last fromToWhere


parseUpdate :: [String] -> Either ErrorMessage ParsedStatement
parseUpdate statement = do
  (_, afterWhere) <- Right $ Data.List.break (== "where") statement
  tableName <- getUpdateTableName statement
  columnAndNewValueString <- getColumnAndNewValueStringsForUpdate statement
  selectedColumns <- getSelectedColumnsForUpdate columnAndNewValueString tableName
  updateValues <- getValuesToUpdate columnAndNewValueString
  whereClause <- statementClause' afterWhere tableName
  getUpdateStatement tableName selectedColumns updateValues whereClause

-- Why did I use `do` here? Well... Thats a good question that I don't know the answer to
getUpdateTableName :: [String] -> Either ErrorMessage TableName
getUpdateTableName statement = do
  (beforeSet, _) <- Right $ Data.List.break (== "set") statement
  tableToUpdate <- Right $ Data.List.drop 1 beforeSet
  if isUpdateTableNameValid tableToUpdate
      then Right $ Data.List.head tableToUpdate
      else Left "Update statement table name does not meet requirements. Maybe ilegal characters were used?"

isUpdateTableNameValid :: [String] -> Bool
isUpdateTableNameValid table = Data.List.length table == 1 && isValidTable (Data.List.head table)

getColumnAndNewValueStringsForUpdate :: [String] -> Either ErrorMessage [String]
getColumnAndNewValueStringsForUpdate updateStatement = do
  (_, setAndAfter) <- Right $ Data.List.break (== "set") updateStatement
  (fromSetToWhere, _) <- Right $ Data.List.break (== "where") setAndAfter
  cleanedFromSetToWhere <- Right $ cleanLastComma fromSetToWhere
  Right $ Data.List.drop 1 cleanedFromSetToWhere

cleanLastComma :: [String] -> [String]
cleanLastComma = Data.List.map (\str -> if Data.List.last str == ',' then Data.List.init str else str)

getSelectedColumnsForUpdate :: [String] -> TableName -> Either ErrorMessage [SelectColumn]
getSelectedColumnsForUpdate [val1, op, val2] tableName = do
  baseColumn <- getValidUpdateColumn val1 op val2 tableName
  return [baseColumn]

getSelectedColumnsForUpdate (val1 : op : val2 : xs) tableName = do
  baseColumn <- getValidUpdateColumn val1 op val2 tableName
  rest <- getSelectedColumnsForUpdate xs tableName
  return $ rest ++ [baseColumn]

getSelectedColumnsForUpdate _ _ = Left "Invalid column formating for UPDATE statement"

getValidUpdateColumn :: String -> String -> String -> TableName -> Either ErrorMessage SelectColumn
getValidUpdateColumn val1 op val2 tableName
  | op == "=" && checkIfDoesNotContainSpecialSymbols val1 && (isNumber val2 || val2 == "true" || val2 == "false" || ("'" `Data.List.isPrefixOf` val2 && "'" `Data.List.isSuffixOf` val2 )) = Right $ TableColumn tableName val1
  | otherwise = Left "Unable to update table do to bad update statement. Failed to parse new column values"

checkIfDoesNotContainSpecialSymbols :: String -> Bool
checkIfDoesNotContainSpecialSymbols val1 = notElem '\'' val1 && notElem '.' val1 && notElem '(' val1 && notElem ')' val1

getValuesToUpdate :: [String] -> Either ErrorMessage [Value]
getValuesToUpdate [val1, op, val2] = do
  baseColumn <- getValidUpdateValue val1 op val2
  return [baseColumn]

getValuesToUpdate (val1 : op : val2 : xs) = do
  baseColumn <- getValidUpdateValue val1 op val2
  rest <- getValuesToUpdate xs
  return $ rest ++ [baseColumn]

getValuesToUpdate _  = Left "Invalid column formating for UPDATE statement"

getValidUpdateValue :: String -> String -> String -> Either ErrorMessage Value
getValidUpdateValue val1 op val2
  | op == "=" && checkIfDoesNotContainSpecialSymbols val1 && (isNumber val2 || val2 == "true" || val2 == "false" || ("'" `Data.List.isPrefixOf` val2 && "'" `Data.List.isSuffixOf` val2 )) = getValueFromString val2
  | otherwise = Left "Unable to update table do to bad update statement. Failed to parse columns to be updated"

getValueFromString :: String -> Either ErrorMessage Value
getValueFromString valueString
  | valueString == "true" = Right $ BoolValue True
  | valueString == "false" = Right $ BoolValue False
  | isNumber valueString = Right $ IntegerValue (read valueString :: Integer)
  | "'" `Data.List.isPrefixOf` valueString && "'" `Data.List.isSuffixOf` valueString = Right $ StringValue $ Data.List.drop 1 (Data.List.init valueString)
  | otherwise = Left "Failed to parse UPDATE statement value. Only string, integer and bool values allowed"

getUpdateStatement :: TableName -> [SelectColumn] -> Row -> Maybe WhereClause -> Either ErrorMessage ParsedStatement
getUpdateStatement tableName selectedColumns rows whereClause = Right $ UpdateStatement tableName selectedColumns rows whereClause


parseShowTables :: [String] -> Either ErrorMessage ParsedStatement
parseShowTables ["show", "tables"] = Right ShowTablesStatement
parseShowTables _ = Left "Failed to parse SHOW TABLES statement"

parseShowTable :: [String] -> Either ErrorMessage ParsedStatement
parseShowTable ["show", "table", tableName] = do
  if isValidTable tableName
    then do
      return $ ShowTableStatement tableName
    else do
      Left $ "SHOW TABLE table name contains ilegal characters"++ " " ++ tableName

parseShowTable _ = Left "Failed to parse SHOW TABLE statement"

isValidTable :: String -> Bool
isValidTable originalName
  | "'" `Data.List.isPrefixOf` originalName || "'" `Data.List.isSuffixOf` originalName || "avg(" `Data.List.isPrefixOf` originalName || "max(" `Data.List.isPrefixOf` originalName || ")" `Data.List.isSuffixOf` originalName = False
  | otherwise = True

parseSelect :: [String] -> Either ErrorMessage ParsedStatement
parseSelect statement = do
  tableNamesAndAb <- getTableNamesAndAb statement
  (beforeWhere, afterWhere) <- Right $ Data.List.break (== "where") statement
  (beforeFrom, _) <- Right $ Data.List.break (== "from") beforeWhere
  whereClause <- statementClause afterWhere
  selectType <- getSelectType beforeFrom
  tableNames <- Right $ Data.List.map fst tableNamesAndAb
  case selectType of
    Aggregate -> do
      aggregateColumns <- getAggregateColumns beforeFrom tableNamesAndAb
      parseAggregate tableNames whereClause aggregateColumns
    ColumnsAndTime -> do
      columnsWithTableAb <- getColumnWithTableAb statement tableNamesAndAb
      parseColumnsSelect tableNames columnsWithTableAb whereClause
    AllColumns -> do
      parseAllColumns tableNames whereClause

castEither :: a -> Either ErrorMessage a -> a
castEither defaultValue eitherValue = case eitherValue of
  Right val -> val
  Left _ -> defaultValue

getAggregateColumns :: [String] -> [(TableName, String)] -> Either ErrorMessage [SelectColumn]
getAggregateColumns [baseColumn] tableNames = do
  parsedAggregate <- parseAggregateColumn baseColumn tableNames
  return [parsedAggregate]
getAggregateColumns (baseColumn : xs) tableNames = do
  baseAggregate <- parseAggregateColumn baseColumn tableNames
  rest <- getAggregateColumns xs tableNames
  return $ baseAggregate : rest
getAggregateColumns _ _ = Left "Error parsing aggregate columns"

parseAggregateColumn :: String -> [(TableName, String)] -> Either ErrorMessage SelectColumn
parseAggregateColumn column tableNames
  | isValid && "avg(" `Data.List.isPrefixOf` column && ")" `Data.List.isSuffixOf` column = Right $ Avg tableAb columnName
  | isValid && "max(" `Data.List.isPrefixOf` column && ")" `Data.List.isSuffixOf` column = Right $ Max tableAb columnName
  | otherwise = Left $ "Failed to parse aggregate column" ++ column
    where
      removedAggregate = Data.List.drop 4 (Data.List.init column)
      [tableAb, columnName] = wordsWhen (== '.') removedAggregate
      isValid = findIfTupleWithSndElemEqualToExists tableAb tableNames

findIfTupleWithSndElemEqualToExists :: Eq a => a -> [(b, a)] -> Bool
findIfTupleWithSndElemEqualToExists _ [] = False;
findIfTupleWithSndElemEqualToExists val [(_, sndVal)] = val == sndVal
findIfTupleWithSndElemEqualToExists val ((_, sndVal) : xs) = (val == sndVal) || findIfTupleWithSndElemEqualToExists val xs


getTableNamesAndAb :: [String] -> Either ErrorMessage [(TableName, String)]
getTableNamesAndAb statement = names
  where
    (_, afterFrom) = Data.List.break (== "from") statement
    (tables, _) = Data.List.break (== "where") afterFrom
    dropedTable = Data.List.drop 1 tables
    len = Data.List.length dropedTable

    names
      | even len && len > 0 = splitIntoTuples dropedTable
      | otherwise = Left "Invalid table formating in statement. Maybe table abbreviation was not provided?"

-- Look into and fix check if Data.List.all columns ar valid. Will do for now because of deadline but this method is shit
getColumnWithTableAb :: [String] -> [(TableName, String)] -> Either ErrorMessage [(String, String)]
getColumnWithTableAb statement tableNames = do
  (beforeFrom, _) <- Right $ Data.List.break (== "from") statement
  columnAndAbList <- Right $ getListOfTableAbAndColumn beforeFrom
  isValidColumnNames <- Right $ Data.List.all isValidColumn beforeFrom
  if Data.List.null columnAndAbList || odd (Data.List.length columnAndAbList) || not isValidColumnNames
    then do
      Left $ "error parsing columns. Maybe table name abbreviation was not provided? : " ++ Data.List.head columnAndAbList 
    else do
      splitList <- splitIntoTuples columnAndAbList
      getCorrectTableAndColumns splitList tableNames

getCorrectTableAndColumns :: [(String, String)] -> [(String, String)] -> Either ErrorMessage [(String, String)]

getCorrectTableAndColumns [columnAndTableAb] tableNames = do
  tableName <- getFstTupleElemBySndElemInList columnAndTableAb tableNames
  return [(tableName, snd columnAndTableAb)]

getCorrectTableAndColumns (columnAndTableAb : xs) tableNames = do
  tableExists <- Right $ findIfTupleWithSndElemEqualToExists (snd columnAndTableAb) tableNames
  if tableExists
    then do
      tableName <- getFstTupleElemBySndElemInList columnAndTableAb tableNames
      rest <- getCorrectTableAndColumns xs tableNames
      return $ (tableName, snd columnAndTableAb) : rest
    else
      getCorrectTableAndColumns xs tableNames

getCorrectTableAndColumns _ _ = Left "Something went wrong when trying to find match for table abbreviation"

getFstTupleElemBySndElemInList :: Eq a => (a, b) -> [(c, a)] -> Either ErrorMessage c

getFstTupleElemBySndElemInList  (val1, _) [(val3, val2)] = if val1 == val2 then Right val3 else Left "Failed to find table name that matches one of the abbriviations"

getFstTupleElemBySndElemInList val1 (x : xs) = do
  let isMatch (src, _) (_, src') = src == src'
  isFirstMatch <- Right $ isMatch val1 x
  if isFirstMatch
    then Right $ fst x
    else getFstTupleElemBySndElemInList val1 xs

getFstTupleElemBySndElemInList _ _ = Left "Failed to find valid table that matches table abbreviation"


getListOfTableAbAndColumn :: [String] -> [String]
getListOfTableAbAndColumn [] = []
getListOfTableAbAndColumn [abAndColumn] = wordsWhen (== '.') abAndColumn
getListOfTableAbAndColumn (abAndColumn : xs) = wordsWhen (== '.') abAndColumn ++ getListOfTableAbAndColumn xs


wordsWhen :: (Char -> Bool) -> String -> [String]
wordsWhen p s = case Data.List.dropWhile p s of
  "" -> []
  s' -> w : wordsWhen p s''
        where (w, s'') = Data.List.break p s'


splitIntoTuples :: [String] -> Either ErrorMessage [(String, String)]
splitIntoTuples [x, y] = Right [(x, y)]
splitIntoTuples (x : y : xs) = do
  baseTuple <- Right (x, y)
  rest <- splitIntoTuples xs
  return $ baseTuple : rest

splitIntoTuples _ = Left "Error parsing tables or columns. Maybe table abbreviation was not provided?"

parseAggregate :: [TableName] -> Maybe WhereClause -> [SelectColumn] -> Either ErrorMessage ParsedStatement
parseAggregate tableNames whereClause columns = Right $ SelectAggregate tableNames columns whereClause

-- ignores commas in columns
parseColumnsSelect :: [TableName] -> [(String, String)] -> Maybe WhereClause -> Either ErrorMessage ParsedStatement
parseColumnsSelect tables columns whereClause = Right $ SelectColumns tables (Data.List.map (uncurry TableColumn) columns) whereClause

parseAllColumns :: [TableName] -> Maybe WhereClause -> Either ErrorMessage ParsedStatement
parseAllColumns tableNames whereClause = Right $ SelectAll tableNames whereClause

getSelectType :: [String] -> Either ErrorMessage SelectType
getSelectType selectColumns
  | hasAllValidAggregates selectColumns = Right Aggregate
  | hasAllValidColumns selectColumns = Right ColumnsAndTime
  | isValidSelectAll selectColumns = Right AllColumns
  | otherwise = Left "Error parsing select type. Maybe invalid table abbreviation was used or an invalid query was given"

isValidSelectAll :: [String] -> Bool
isValidSelectAll selectColumns
  | Data.List.length selectColumns == 1 && Data.List.head selectColumns == "*" = True
  | otherwise = False

hasAllValidColumns :: [String] -> Bool
hasAllValidColumns [] = False
hasAllValidColumns [column] = isValidColumn column
hasAllValidColumns (column : xs) = isValidColumn column && hasAllValidColumns xs

isValidColumn :: String -> Bool
isValidColumn column = isSplitByDot && isNotAggregate && hasNoParenthesies || column == "now()"
  where
    (beforeDot, afterDot) = Data.List.break (== '.') column
    isSplitByDot = not (Data.List.null beforeDot) && not (Data.List.null afterDot) && (Data.List.length afterDot > 1)
    isNotAggregate = not $ isAggregate column
    hasNoParenthesies = not ("'" `Data.List.isPrefixOf` column || "'" `Data.List.isSuffixOf` column)

isValidColumnWithoutAb :: String -> Bool
isValidColumnWithoutAb column = isNotAggregate && hasNoParenthesies || column == "now()"
  where
    isNotAggregate = not $ "avg(" `Data.List.isPrefixOf` column || "max(" `Data.List.isPrefixOf` column || "'" `Data.List.isSuffixOf` column
    hasNoParenthesies = not ("'" `Data.List.isPrefixOf` column || "'" `Data.List.isSuffixOf` column)

hasAllValidAggregates :: [String] -> Bool
hasAllValidAggregates [] = False
hasAllValidAggregates [column] = isAggregate column
hasAllValidAggregates (column : xs) = isAggregate column && hasAllValidAggregates xs

isAggregate :: String -> Bool
isAggregate column
  | ("max(" `Data.List.isPrefixOf` column || "avg(" `Data.List.isPrefixOf` column) && ")" `Data.List.isSuffixOf` column && isValidColumn (Data.List.drop 4 (Data.List.init column)) = True
  | otherwise = False

statementClause :: [String] -> Either ErrorMessage (Maybe WhereClause)
statementClause afterWhere = do
  case Data.List.length afterWhere of
    0 -> do
      Right Nothing
    _ -> do
      tryParseWhereClause afterWhere

tryParseWhereClause :: [String] -> Either ErrorMessage (Maybe WhereClause)
tryParseWhereClause afterWhere
  | isBoolIsTrueFalseClauseLike = case splitStatementToWhereIsClause (Data.List.drop 1 afterWhere) of
    Left err -> Left err
    Right clause -> Right $ Just clause
  | isAndClauseLike = case parseWhereAnd (Data.List.drop 1 afterWhere) of
    Left err -> Left err
    Right clause -> Right $ Just clause
  | otherwise = Left "Failed to parse where clause. Where clause type not implemented or recognised. Please only use `where and` and `where bool is true/false`"
  where
    afterWhereWithoutWhere = Data.List.drop 1 afterWhere
    (_, afterIs) = Data.List.break (== "is") afterWhereWithoutWhere
    firstElementIsColumn = not (Data.List.null afterWhereWithoutWhere) && (Data.List.length (wordsWhen (== '.') (Data.List.head afterWhereWithoutWhere)) == 2)
    isBoolIsTrueFalseClauseLike = Data.List.length afterWhereWithoutWhere == 3 && Data.List.length afterIs == 2 && (afterIs !! 1 == "false" || afterIs !! 1 == "true") && firstElementIsColumn
    isAndClauseLike = Data.List.null afterIs

splitStatementToWhereIsClause :: [String] -> Either ErrorMessage WhereClause
splitStatementToWhereIsClause [boolColName, "is", boolString] = Right $ IsValueBool parsedBoolString tableName $ Data.List.init colName
  where
    (tableName, colName) = Data.List.break (== '.') boolColName
    parsedBoolString = boolString == "true"
splitStatementToWhereIsClause _ = Left "Unsupported or invalid where bool is true false clause"


parseWhereAnd :: [String] -> Either ErrorMessage WhereClause
parseWhereAnd afterWhere
  | matchesWhereAndPattern afterWhere = splitStatementToAndClause afterWhere
  | otherwise = Left "Unable to parse where and clause"
  where
    splitStatementToAndClause :: [String] -> Either ErrorMessage WhereClause
    splitStatementToAndClause strList = do
      conditionList <- getConditionList strList
      getConditions conditionList

    getConditions :: [Condition] -> Either ErrorMessage WhereClause
    getConditions conditions = Right $ Conditions conditions

    getConditionList :: [String] -> Either ErrorMessage [Condition]
    getConditionList [condition1, operator, condition2] = do
      condition <- getCondition condition1 operator condition2
      return [condition]

    getConditionList (condition1 : operator : condition2 : "and" : xs) = do
      conditionBase <- getCondition condition1 operator condition2
      rest <- if not (Data.List.null xs) then getConditionList xs else Right []
      return $ conditionBase : rest

    getConditionList _ = Left "Error parsing where and clause"

getConditionValue :: String -> Either ErrorMessage ConditionValue
getConditionValue condition
  | isNumber condition = Right $ IntValue (read condition :: Integer)
  | Data.List.length condition > 2 && "'" `Data.List.isPrefixOf` condition && "'" `Data.List.isSuffixOf` condition = Right $ StrValue (Data.List.drop 1 (Data.List.init condition))
  | otherwise = Left "Error parsing condition value"

getCondition :: String -> String -> String -> Either ErrorMessage Condition
getCondition val1 op val2
  | op == "=" && val1HasDot && isJust value2Type && isNothing value1Type && isCondition2 = Right $ Equals (TableColumn val1TableAb (Data.List.drop 1 val1Column)) $ castEither defaultCondition condition2
  | op == "=" && val2HasDot && isJust value1Type && isNothing value2Type && isCondition1= Right $ Equals (TableColumn val2TableAb (Data.List.drop 1 val2Column)) $ castEither defaultCondition condition1
  | op == ">" && val1HasDot && isJust value2Type && isNothing value1Type && isCondition2 = Right $ GreaterThan (TableColumn val1TableAb (Data.List.drop 1 val1Column)) $ castEither defaultCondition condition2
  | op == ">" && val2HasDot && isJust value1Type && isNothing value2Type && isCondition1 = Right $ GreaterThan (TableColumn val2TableAb (Data.List.drop 1 val2Column)) $ castEither defaultCondition condition1
  | op == "<" && val1HasDot && isJust value2Type && isNothing value1Type && isCondition2 = Right $ LessThan (TableColumn val1TableAb (Data.List.drop 1 val1Column)) $ castEither defaultCondition condition2
  | op == "<" && val2HasDot && isJust value1Type && isNothing value2Type && isCondition1 = Right $ LessThan (TableColumn val2TableAb (Data.List.drop 1 val2Column)) $ castEither defaultCondition condition1
  | op == ">=" && val1HasDot && isJust value2Type && isNothing value1Type && isCondition2 = Right $ GreaterThanOrEqual (TableColumn val1TableAb (Data.List.drop 1 val1Column)) $ castEither defaultCondition condition2
  | op == ">=" && val2HasDot && isJust value1Type && isNothing value2Type && isCondition1 = Right $ GreaterThanOrEqual (TableColumn val2TableAb (Data.List.drop 1 val2Column)) $ castEither defaultCondition condition1
  | op == "<=" && val1HasDot && isJust value2Type && isNothing value1Type && isCondition2 = Right $ LessthanOrEqual (TableColumn val1TableAb (Data.List.drop 1 val1Column)) $ castEither defaultCondition condition2
  | op == "<=" && val2HasDot && isJust value1Type && isNothing value2Type && isCondition1 = Right $ LessthanOrEqual (TableColumn val2TableAb (Data.List.drop 1 val2Column)) $ castEither defaultCondition condition1
  | op == "<>" && val1HasDot && isJust value2Type && isNothing value1Type && isCondition2 = Right $ NotEqual (TableColumn val1TableAb (Data.List.drop 1 val1Column)) $ castEither defaultCondition condition2
  | op == "<>" && val2HasDot && isJust value1Type && isNothing value2Type && isCondition1 = Right $ NotEqual (TableColumn val2TableAb (Data.List.drop 1 val2Column)) $ castEither defaultCondition condition1
  | otherwise = Left "Error parsing where and condition. Only able to compare integer and string values with columns"
  where
    (val1TableAb, val1Column) = Data.List.break (=='.') val1
    (val2TableAb, val2Column) = Data.List.break (=='.') val2
    value1Type = parseType val1
    value2Type = parseType val2
    val1HasDot = not (Data.List.null (Data.List.drop 1 val1Column))
    val2HasDot = not (Data.List.null (Data.List.drop 1 val2Column))
    condition1 = getConditionValue val1
    condition2 = getConditionValue val2
    defaultCondition = StrValue "Kas skaitys tas gaidys (Isskyrus destytoja)"

    isCondition1 = case condition1 of
      Right _ -> True
      Left _ -> False

    isCondition2 = case condition2 of
      Right _ -> True
      Left _ -> False


matchesWhereAndPattern :: [String] -> Bool
matchesWhereAndPattern [condition1, operator, condition2] = isWhereAndOperation condition1 operator condition2
matchesWhereAndPattern (condition1 : operator : condition2 : andString : xs) = matchesWhereAndPattern [condition1, operator, condition2] && andString == "and" && matchesWhereAndPattern xs
matchesWhereAndPattern _ = False

isWhereAndOperation :: String -> String -> String -> Bool
isWhereAndOperation condition1 operator condition2
  | Data.List.elem operator [">", "<", "=", "<>", "<=", ">="] && (col1Valid || col2Valid) = True
  | otherwise = False
  where
    (val1Table, val1Column) = Data.List.break (== '.') condition1
    (val2Table, val2Column) = Data.List.break (== '.') condition2
    val1ColumWithoutDot = Data.List.drop 1 val1Column
    val2ColumWithoutDot = Data.List.drop 1 val2Column
    col1Valid = not (Data.List.null val1ColumWithoutDot) && not (Data.List.null val1Table)
    col2Valid = not (Data.List.null val2ColumWithoutDot) && not (Data.List.null val2Table)


statementClause' :: [String] -> TableName -> Either ErrorMessage (Maybe WhereClause)
statementClause' afterWhere tableName = do
  case Data.List.length afterWhere of
    0 -> do
      Right Nothing
    _ -> do
      tryParseWhereClause' afterWhere tableName

tryParseWhereClause' :: [String] -> TableName -> Either ErrorMessage (Maybe WhereClause)
tryParseWhereClause' afterWhere tableName
  | isBoolIsTrueFalseClauseLike = case splitStatementToWhereIsClause' afterWhere tableName of
    Left err -> Left err
    Right clause -> Right $ Just clause
  | isAndClauseLike = case parseWhereAnd' (Data.List.drop 1 afterWhere) tableName of
    Left err -> Left err
    Right clause -> Right $ Just clause
  | otherwise = Left "Failed to parse where clause. Where clause type not implemented or recognised. Please only use `where and` and `where bool is true/false`"
  where
    afterWhereWithoutWhere = Data.List.drop 1 afterWhere
    (_, afterIs) = Data.List.break (== "is") afterWhereWithoutWhere
    firstElementIsColumn = not (Data.List.null afterWhereWithoutWhere) && (Data.List.length (wordsWhen (== '.') (Data.List.head afterWhereWithoutWhere)) == 1)
    isBoolIsTrueFalseClauseLike = Data.List.length afterWhereWithoutWhere == 3 && Data.List.length afterIs == 2 && (afterIs !! 1 == "false" || afterIs !! 1 == "true") && firstElementIsColumn
    isAndClauseLike = Data.List.null afterIs

splitStatementToWhereIsClause' :: [String] -> TableName -> Either ErrorMessage WhereClause
splitStatementToWhereIsClause' [boolColName, "is", boolString] tableName = Right $ IsValueBool parsedBoolString tableName $ Data.List.init boolColName
  where
    parsedBoolString = boolString == "true"
splitStatementToWhereIsClause' _ _ = Left "Unsupported or invalid where bool is true false clause"


parseWhereAnd' :: [String] -> TableName -> Either ErrorMessage WhereClause
parseWhereAnd' afterWhere tableName
  | matchesWhereAndPattern' afterWhere = splitStatementToAndClause afterWhere tableName
  | otherwise = Left "Unable to parse where and clause"
  where
    splitStatementToAndClause :: [String] -> TableName -> Either ErrorMessage WhereClause
    splitStatementToAndClause strList tableName' = do
      conditionList <- getConditionList strList tableName'
      getConditions conditionList

    getConditions :: [Condition] -> Either ErrorMessage WhereClause
    getConditions conditions = Right $ Conditions conditions

    getConditionList :: [String] -> TableName -> Either ErrorMessage [Condition]
    getConditionList [condition1, operator, condition2] tableName'' = do
      condition <- getCondition' condition1 operator condition2 tableName''
      return [condition]

    getConditionList (condition1 : operator : condition2 : "and" : xs) tableName'' = do
      conditionBase <- getCondition' condition1 operator condition2 tableName''
      rest <- if not (Data.List.null xs) then getConditionList xs tableName'' else Right []
      return $ conditionBase : rest

    getConditionList _ _ = Left "Error parsing where and clause"

getConditionValue' :: String -> Either ErrorMessage ConditionValue
getConditionValue' condition
  | isNumber condition = Right $ IntValue (read condition :: Integer)
  | Data.List.length condition > 2 && "'" `Data.List.isPrefixOf` condition && "'" `Data.List.isSuffixOf` condition = Right $ StrValue (Data.List.drop 1 (Data.List.init condition))
  | otherwise = Left "Error parsing condition value"

getCondition' :: String -> String -> String -> TableName -> Either ErrorMessage Condition
getCondition' val1 op val2 tableName
  | op == "=" && isJust value2Type && isNothing value1Type && isCondition2 = Right $ Equals (TableColumn tableName val1) $ castEither defaultCondition condition2
  | op == "=" && isJust value1Type && isNothing value2Type && isCondition1= Right $ Equals (TableColumn tableName val2) $ castEither defaultCondition condition1
  | op == ">" && isJust value2Type && isNothing value1Type && isCondition2 = Right $ GreaterThan (TableColumn tableName val1) $ castEither defaultCondition condition2
  | op == ">" && isJust value1Type && isNothing value2Type && isCondition1 = Right $ GreaterThan (TableColumn tableName val2) $ castEither defaultCondition condition1
  | op == "<" && isJust value2Type && isNothing value1Type && isCondition2 = Right $ LessThan (TableColumn tableName val1) $ castEither defaultCondition condition2
  | op == "<" && isJust value1Type && isNothing value2Type && isCondition1 = Right $ LessThan (TableColumn tableName val2) $ castEither defaultCondition condition1
  | op == ">=" && isJust value2Type && isNothing value1Type && isCondition2 = Right $ GreaterThanOrEqual (TableColumn tableName val1) $ castEither defaultCondition condition2
  | op == ">=" && isJust value1Type && isNothing value2Type && isCondition1 = Right $ GreaterThanOrEqual (TableColumn tableName val2) $ castEither defaultCondition condition1
  | op == "<=" && isJust value2Type && isNothing value1Type && isCondition2 = Right $ LessthanOrEqual (TableColumn tableName val1) $ castEither defaultCondition condition2
  | op == "<=" && isJust value1Type && isNothing value2Type && isCondition1 = Right $ LessthanOrEqual (TableColumn tableName val2) $ castEither defaultCondition condition1
  | op == "<>" && isJust value2Type && isNothing value1Type && isCondition2 = Right $ NotEqual (TableColumn tableName val1) $ castEither defaultCondition condition2
  | op == "<>" && isJust value1Type && isNothing value2Type && isCondition1 = Right $ NotEqual (TableColumn tableName val2) $ castEither defaultCondition condition1
  | otherwise = Left "Error parsing where and condition. Only able to compare integer and string values with columns"
  where
    value1Type = parseType val1
    value2Type = parseType val2
    condition1 = getConditionValue' val1
    condition2 = getConditionValue' val2
    defaultCondition = StrValue "Kas skaitys tas gaidys (Isskyrus destytoja)"

    isCondition1 = case condition1 of
      Right _ -> True
      Left _ -> False

    isCondition2 = case condition2 of
      Right _ -> True
      Left _ -> False


matchesWhereAndPattern' :: [String] -> Bool
matchesWhereAndPattern' [condition1, operator, condition2] = isWhereAndOperation' condition1 operator condition2
matchesWhereAndPattern' (condition1 : operator : condition2 : andString : xs) = matchesWhereAndPattern' [condition1, operator, condition2] && andString == "and" && matchesWhereAndPattern' xs
matchesWhereAndPattern' _ = False

isWhereAndOperation' :: String -> String -> String -> Bool
isWhereAndOperation' condition1 operator condition2
  | Data.List.elem operator [">", "<", "=", "<>", "<=", ">="] && (col1Valid || col2Valid) = True
  | otherwise = False
  where
    col1Valid = not (Data.List.null condition1)
    col2Valid = not (Data.List.null condition2)

parseType :: String -> Maybe ColumnType
parseType str
  | isNumber str = Just IntegerType
  | "'" `Data.List.isPrefixOf` str && "'" `Data.List.isSuffixOf` str = Just StringType
  | otherwise = Nothing

isNumber :: String -> Bool
isNumber "" = False
isNumber xs =
  case Data.List.dropWhile isDigit xs of
    "" -> True
    _ -> False

parseSemiCaseSensitive :: String -> [String]
parseSemiCaseSensitive statement = convertedStatement
  where
    splitStatement = Data.List.words statement
    convertedStatement = Data.List.map wordToLowerSensitive splitStatement

wordToLowerSensitive :: String -> String
wordToLowerSensitive word
  | Data.List.map Data.Char.toLower word `Data.List.elem` keywords = Data.List.map Data.Char.toLower word
  | "avg(" `Data.List.isPrefixOf` Data.List.map Data.Char.toLower word && ")" `Data.List.isSuffixOf` word = "avg(" ++ Data.List.drop 4 (Data.List.init word) ++ ")"
  | "max(" `Data.List.isPrefixOf` Data.List.map Data.Char.toLower word && ")" `Data.List.isSuffixOf` word = "max(" ++ Data.List.drop 4 (Data.List.init word) ++ ")"
  | otherwise = word
  where
    keywords = ["select", "from", "where", "show", "table", "tables", "false", "true", "and", "is", "insert", "delete", "update", "set", "into"]

