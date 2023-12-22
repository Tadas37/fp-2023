{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE RankNTypes #-}

{-# LANGUAGE InstanceSigs #-}

module Lib4
  ( executeSql,
    insertRows,
    deleteRows,
    updateRows,
    parseTables,
    Execution,
    ExecutionAlgebra(..),
    ParsedStatement(..),
    SelectColumn(..),
    TableName,
    loadFiles,
    getTime,
    parseYAMLContent,
    dataFrameToSerializedTable,
    serializeTableToYAML,
    validateStatement,
    SerializedTable(..),
    updateRowsInTable,
    updateRowValues,
    parseStatement,
    filterRows,
    deleteRowsFromDataFrame,
    rowSatisfiesWhereClause,
    compareWithCondition,
    compareValue,
    findColumnIndex,
    extractSelectedColumnsRows,
    extractAggregateRows,
    extractColumnNames,
    WhereClause(..),
    Condition(..),
    ConditionValue(..),
    getSelectedColumns,
    showTableFunction,
    showTablesFunction,
    getStatementType,
    getTableDfByName,
    StatementType(..),
    getNonSelectTableNameFromStatement,
    parseSql,
    SelectedColumns,
    generateDataFrame,
    getReturnTableRows,
    getTableNames,
  )
where

import Control.Monad.Free (Free (..), liftF)
import Control.Monad.State
import DataFrame (Column(..), ColumnType(..), Value(..), Row, DataFrame(..))
import Data.Yaml (decodeEither')
import Data.Text as T ( unpack, pack )
import Data.List
    ( (++),
      zip,
      map,
      elemIndex,
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
      isSuffixOf,
      partition,
      concatMap,
      find,
      lookup,
      any,
      notElem,
      findIndex,
      (!!),
      isInfixOf, 
      sortBy )
import qualified Data.Yaml as Y
import Data.Char (toLower, isDigit)
import Data.Time (UTCTime)
import qualified Data.Aeson.Key as Key
import qualified Data.ByteString.Char8 as BS
import Data.Either (isRight, isLeft, partitionEithers)
import Prelude hiding (zip)
import Data.Maybe ( mapMaybe, isJust, isNothing, fromMaybe )

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
  | Max TableName ColumnName
  | Avg TableName ColumnName
  deriving (Show, Eq)

type SelectedColumns = [SelectColumn]
type SelectedTables = [TableName]

data ParsedStatement
  = SelectAll SelectedTables (Maybe WhereClause) (Maybe SortClause)
  | SelectAggregate SelectedTables SelectedColumns (Maybe WhereClause) (Maybe SortClause)
  | SelectColumns SelectedTables SelectedColumns (Maybe WhereClause) (Maybe SortClause)
  | DeleteStatement TableName (Maybe WhereClause)
  | InsertStatement TableName (Maybe SelectedColumns) Row
  | UpdateStatement TableName SelectedColumns Row (Maybe WhereClause)
  | ShowTableStatement TableName
  | ShowTablesStatement
  | CreateTableStatement TableName [Column]
  | DropTableStatement TableName
  | Invalid ErrorMessage
  | SelectStatement
      { selectedTables :: SelectedTables
      , selectedColumns :: Maybe SelectedColumns
      , whereClause :: Maybe WhereClause
      , orderClause :: Maybe OrderClause 
      }
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
  | ColumnValuesEqual SelectColumn SelectColumn
  deriving (Show, Eq)

data ConditionValue
  = StrValue String
  | IntValue Integer
  deriving (Show, Eq)

data SortClause
  = ColumnSort SelectedColumns (Maybe SortOrder)
  deriving (Show, Eq)

data SortOrder
  = Asc
  | Desc
  deriving (Show, Eq)

type OrderClause = [(SortOrder, Maybe TableName, ColumnName)]

newtype EitherT e m a = EitherT {
    runEitherT :: m (Either e a)
}

instance Functor m => Functor (EitherT e m) where
  fmap f (EitherT m) = EitherT $ fmap (fmap f) m

instance Applicative m => Applicative (EitherT e m) where
  pure x = EitherT $ pure (Right x)
  (EitherT f) <*> (EitherT x) = EitherT $ (<*>) <$> f <*> x

instance Monad m => Monad (EitherT e m) where
  return = pure
  (EitherT x) >>= f = EitherT $ do
    result <- x
    case result of
      Left e -> return (Left e)
      Right y -> runEitherT (f y)

instance MonadTrans (EitherT e) where
  lift :: Monad m => m a -> EitherT e m a
  lift ma = EitherT $ fmap Right ma

type ParsedStatementT a = EitherT ErrorMessage (State [String]) a

data SelectType = Aggregate | ColumnsAndTime | AllColumns

data StatementType = Select | Delete | Insert | Update | ShowTable | ShowTables | InvalidStatement | CreateTable | DropTable

data ExecutionAlgebra next
  = LoadFiles [TableName] (Either ErrorMessage [FileContent] -> next)
  | UpdateTable (TableName, DataFrame) next
  | GetTime (UTCTime -> next)
  | RemoveTable TableName (Maybe ErrorMessage -> next)
  | CreateTablee TableName [Column] (Maybe ErrorMessage -> next)
  deriving Functor

type Execution = Free ExecutionAlgebra

loadFiles :: [TableName] -> Execution (Either ErrorMessage [FileContent])
loadFiles names = liftF $ LoadFiles names id

updateTable :: (TableName, DataFrame) -> Execution ()
updateTable table = liftF $ UpdateTable table ()

getTime :: Execution UTCTime
getTime = liftF $ GetTime id

removeTable :: TableName -> Execution (Maybe ErrorMessage)
removeTable tableName = liftF $ RemoveTable tableName id

createTablee :: TableName -> [Column] -> Execution (Maybe ErrorMessage)
createTablee tableName columns = liftF (CreateTablee tableName columns id)

parseTables :: [FileContent] -> Either String [(TableName, DataFrame)]
parseTables contents =
    let parsedTables = map parseYAMLContent contents
        (errors, tables) = partitionEithers parsedTables
    in if null errors
        then Right tables
        else Left $ "YAML parsing errors: " ++ intercalate "; " errors


getTableDfByName :: TableName -> [(TableName, DataFrame)] -> Either ErrorMessage DataFrame
getTableDfByName tableName tables =
    case lookup tableName tables of
        Just df -> Right df
        Nothing -> Left $ "Table not found: " ++ tableName

parseSql :: SQLQuery -> Either ErrorMessage ParsedStatement
parseSql query =
    case parseStatement query of
        Right parsedStatement -> Right parsedStatement
        Left err -> Left err

getTableNames :: Lib4.ParsedStatement -> [Lib4.TableName]
getTableNames (Lib4.SelectAll tableNames _ _) = tableNames
getTableNames (Lib4.SelectAggregate tableNames _ _ _) = tableNames
getTableNames (Lib4.SelectColumns tableNames _ _ _) = tableNames
getTableNames (Lib4.DropTableStatement tableNames ) = [tableNames]
getTableNames (Lib4.CreateTableStatement _ _) = [];
getTableNames (Lib4.DeleteStatement tableName _) = [tableName]
getTableNames (Lib4.InsertStatement tableName _ _) = [tableName]
getTableNames (Lib4.UpdateStatement tableName _ _ _) = [tableName]
getTableNames (Lib4.ShowTableStatement tableName) = [tableName]
getTableNames Lib4.ShowTablesStatement = ["employees", "animals"]
getTableNames (Lib4.Invalid _) = []

deleteRows :: Lib4.ParsedStatement -> [(Lib4.TableName, DataFrame)] -> Either ErrorMessage (Lib4.TableName, DataFrame)
deleteRows (Lib4.DeleteStatement tableName whereClause) tables =
    case lookup tableName tables of
        Just df ->
            case Lib4.filterRows df whereClause of
                Right dfFiltered -> Right (tableName, dfFiltered)
                Left errMsg -> Left errMsg
        Nothing -> Left $ "Table not found: " ++ tableName
deleteRows (Lib4.SelectAll {}) _ = Left "SelectAll not valid for DeleteRows"
deleteRows (Lib4.SelectAggregate {}) _ = Left "SelectAggregate not valid for DeleteRows"
deleteRows (Lib4.SelectColumns {}) _ = Left "SelectColumns not valid for DeleteRows"
deleteRows (Lib4.InsertStatement {}) _ = Left "InsertStatement not valid for DeleteRows"
deleteRows (Lib4.UpdateStatement {}) _ = Left "UpdateStatement not valid for DeleteRows"
deleteRows (Lib4.ShowTableStatement _) _ = Left "ShowTableStatement not valid for DeleteRows"
deleteRows Lib4.ShowTablesStatement _ = Left "ShowTablesStatement not valid for DeleteRows"
deleteRows (Lib4.Invalid _) _ = Left "Invalid statement cannot be processed in DeleteRows"

insertRows :: Lib4.ParsedStatement -> [(Lib4.TableName, DataFrame)] -> (Lib4.TableName, DataFrame)
insertRows (Lib4.InsertStatement tableName maybeSelectedColumns row) tables =
    case lookup tableName tables of
        Just (DataFrame cols tableRows) ->
          let
            columnNames = fmap Lib4.extractColumnNames maybeSelectedColumns
            justColumnNames = fromMaybe [] columnNames
            newRow = Lib4.createRowFromValues justColumnNames cols row
          in
            case newRow of
                Right newRowData ->
                    let updatedDf = DataFrame cols (tableRows ++ [newRowData])
                    in (tableName, updatedDf)
                Left errMsg -> error errMsg
        Nothing -> error $ "Table not found: " ++ tableName
insertRows (Lib4.SelectAll {}) _ = error "SelectAll not valid for InsertRows"
insertRows (Lib4.SelectAggregate {}) _ = error "SelectAggregate not valid for InsertRows"
insertRows (Lib4.SelectColumns {}) _ = error "SelectColumns not valid for InsertRows"
insertRows (Lib4.DeleteStatement _ _) _ = error "DeleteStatement not valid for InsertRows"
insertRows (Lib4.UpdateStatement {}) _ = error "UpdateStatement not valid for InsertRows"
insertRows (Lib4.ShowTableStatement _) _ = error "ShowTableStatement not valid for InsertRows"
insertRows Lib4.ShowTablesStatement _ = error "ShowTablesStatement not valid for InsertRows"
insertRows (Lib4.Invalid _) _ = error "Invalid statement cannot be processed in InsertRows"

updateRows :: ParsedStatement -> [(TableName, DataFrame)] -> Either ErrorMessage (TableName, DataFrame)
updateRows (UpdateStatement tableName columns row maybeWhereClause) tables =
    case lookup tableName tables of
        Just df ->
            let updatedDf = updateRowsInTable tableName columns row maybeWhereClause df
            in case updatedDf of
                Right dfUpdated -> Right (tableName, dfUpdated)
                Left errMsg -> Left errMsg
        Nothing -> Left $ "Table not found: " ++ tableName
updateRows (SelectAll {}) _ = Left "SelectAll not valid for UpdateRows"
updateRows (SelectAggregate {}) _ = Left "SelectAggregate not valid for UpdateRows"
updateRows (SelectColumns {}) _ = Left "SelectColumns not valid for UpdateRows"
updateRows (DeleteStatement _ _) _ = Left "DeleteStatement not valid for UpdateRows"
updateRows (ShowTableStatement _) _ = Left "ShowTableStatement not valid for UpdateRows"
updateRows ShowTablesStatement _ = Left "ShowTablesStatement not valid for UpdateRows"
updateRows (InsertStatement {}) _ = Left "InsertStatement not valid for UpdateRows"
updateRows (Invalid _) _ = Left "Invalid statement cannot be processed in UpdateRows"



getReturnTableRows :: ParsedStatement -> [(TableName, DataFrame)] -> UTCTime -> [Row]
getReturnTableRows parsedStatement tables _ =
    case parsedStatement of
        SelectAll _ whereClauseAll _ -> filteredRows
          where
              filterdTablesJointDf = case extractMultipleTableDataframe (map snd tables) of
                Right df -> df
                Left _ -> DataFrame [] []

              filteredRows = case whereClauseAll of
                Just wc -> mapMaybe (\row -> if rowSatisfiesWhereClause wc (getDataFrameColumns filterdTablesJointDf) row then Just row else Nothing) (getDataFrameRows filterdTablesJointDf)
                Nothing -> getDataFrameRows filterdTablesJointDf

        SelectColumns tableNames conditions whereClause _ -> Lib4.extractSelectedColumnsRows tableNames conditions tables whereClause
        SelectAggregate tableNames aggFunc conditions _-> Lib4.extractAggregateRows tableNames aggFunc conditions tables
        _ -> error "Unhandled statement type in getReturnTableRows"


generateDataFrame :: [Column] -> [Row] -> DataFrame
generateDataFrame = DataFrame

showTablesFunction :: [TableName] -> DataFrame
showTablesFunction tables =
    let column = Column "tableName" StringType
        rows = map (\name -> [StringValue name]) tables
    in DataFrame [column] rows


showTableFunction :: DataFrame -> DataFrame
showTableFunction (DataFrame columns _) =
    let newDf = DataFrame [Column "ColumnNames" StringType]
                          (map (\(Column colName _) -> [StringValue colName]) columns)
    in newDf

executeSql :: SQLQuery -> Execution (Either ErrorMessage DataFrame)
executeSql statement = do
    let parsedResult = parseSql statement
    either (return . Left) handleParsedStatement parsedResult
  where
    handleParsedStatement parsedStatement = do
        tableFiles <- loadFiles (getTableNames parsedStatement)
        either (return . Left) (processTableFiles parsedStatement) tableFiles

    processTableFiles parsedStatement content = do
        let parsedTables = parseTables content
        either (return . Left) (executeStatement parsedStatement) parsedTables

    executeStatement parsedStatement tables = do
        let statementType = getStatementType statement
        timeStamp <- getTime
        let (isValid, errorMessage) = validateStatement parsedStatement tables
        if not isValid then
            return $ Left errorMessage
        else
            case statementType of
                Select -> executeSelect parsedStatement tables timeStamp
                CreateTable -> executeCreateTable parsedStatement
                DropTable -> executeDropTable parsedStatement
                Delete -> executeDelete parsedStatement tables
                Insert -> executeInsert parsedStatement tables
                Update -> executeUpdate parsedStatement tables
                ShowTables -> executeShowTables parsedStatement
                ShowTable -> executeShowTable parsedStatement tables
                InvalidStatement -> return $ Left "Invalid statement in executeSQL"

    executeDropTable :: ParsedStatement -> Execution (Either ErrorMessage DataFrame)
    executeDropTable (DropTableStatement tableName) = do
        result <- removeTable tableName
        return $ maybe (Right emptyDataFrame) Left result
    executeDropTable _ = return $ Left "Invalid DROP TABLE statement"
    
    executeCreateTable :: ParsedStatement -> Execution (Either ErrorMessage DataFrame)
    executeCreateTable (CreateTableStatement tableName columns) = do
        result <- createTablee tableName columns
        return $ maybe (Right emptyDataFrame) Left result
    executeCreateTable _ = return $ Left "Invalid CREATE TABLE statement"

    executeSelect parsedStatement tables timeStamp = do
      let rows = getReturnTableRows parsedStatement tables timeStamp
      let columns = getSelectedColumns parsedStatement tables
      let df = generateDataFrame columns rows
      case getOrderClause parsedStatement of
          Just orderClause -> case sortDataFrame df orderClause of
                                  Left errMsg -> return $ Left errMsg
                                  Right sortedDf -> return $ Right sortedDf
          Nothing -> return $ Right df

    getOrderClause :: ParsedStatement -> Maybe [(SortOrder, Maybe TableName, ColumnName)]
    getOrderClause (SelectStatement { orderClause = oc }) = oc
    getOrderClause _ = Nothing
    
    sortDataFrame :: DataFrame -> OrderClause -> Either ErrorMessage DataFrame
    sortDataFrame (DataFrame columns rows) orderClause = 
        case orderClause of
            [] -> Right $ DataFrame columns rows
            (sortOrder, maybeTableName, columnName):_ ->
                case findColumnIndex columnName columns of
                    Just idx -> Right $ DataFrame columns (sortBy (compareRows idx sortOrder) rows)
                    Nothing  -> Left $ "Column not found: " ++ columnName

    findColumnIndex :: ColumnName -> [Column] -> Maybe Int
    findColumnIndex columnName columns = findIndex (\(Column colName _) -> colName == columnName) columns

    compareRows :: Int -> SortOrder -> Row -> Row -> Ordering
    compareRows idx Asc row1 row2 = compareValue (row1 !! idx) (row2 !! idx)
    compareRows idx Desc row1 row2 = compareValue (row2 !! idx) (row1 !! idx)

    compareValue :: Value -> Value -> Ordering
    compareValue (IntegerValue i1) (IntegerValue i2) = Prelude.compare i1 i2
    compareValue (StringValue s1) (StringValue s2) = Prelude.compare s1 s2
    compareValue (BoolValue b1) (BoolValue b2) = Prelude.compare b1 b2
    compareValue NullValue NullValue = EQ
    compareValue (DateTimeValue dt1) (DateTimeValue dt2) = Prelude.compare dt1 dt2
    compareValue _ _ = EQ

    executeDelete parsedStatement tables = do
        let deleteResult = deleteRows parsedStatement tables
        either (return . Left) (\(name, df) -> updateTable (name, df) >> return (Right df)) deleteResult

    executeInsert parsedStatement tables = do
        let (name, df) = insertRows parsedStatement tables
        updateTable (name, df)
        return $ Right df

    executeUpdate parsedStatement tables = do
        let updateResult = updateRows parsedStatement tables
        either (return . Left) (\(name, df) -> updateTable (name, df) >> return (Right df)) updateResult

    executeShowTables parsedStatement = return $ Right (showTablesFunction (getTableNames parsedStatement))

    executeShowTable parsedStatement tables = do
        let tableName = getNonSelectTableNameFromStatement parsedStatement
        either (return . Left) (return . Right . showTableFunction) (getTableDfByName tableName tables)

getNonSelectTableNameFromStatement :: ParsedStatement -> TableName
getNonSelectTableNameFromStatement (ShowTableStatement tableName) = tableName
getNonSelectTableNameFromStatement _ = error "Non-select statement expected"

emptyDataFrame :: DataFrame
emptyDataFrame = DataFrame [] []

matchesColumn :: String -> Column -> Bool
matchesColumn fullName (Column colName _) = colName == fullName

parseYAMLContent :: FileContent -> Either ErrorMessage (TableName, DataFrame)
parseYAMLContent content =
  case decodeEither' (BS.pack content) of
    Left err -> Left $ "YAML parsing error: " Data.List.++ show err
    Right serializedTable -> Right $ convertToDataFrame serializedTable

convertToDataFrame :: SerializedTable -> (TableName, DataFrame)
convertToDataFrame st = (tableName st, DataFrame (Data.List.map convertColumn $ columns st) (convertRows $ rows st))

convertColumn :: SerializedColumn -> Column
convertColumn sc = Column (name sc) (convertColumnType $ dataType sc)

convertColumnType :: String -> ColumnType
convertColumnType dt = case dt of
    "integer" -> IntegerType
    "string" -> StringType
    "boolean" -> BoolType
    _ -> error "Unknown column type"

convertRows :: [[Y.Value]] -> [Row]
convertRows = Data.List.map (Data.List.map convertValue)

getStatementType :: String -> StatementType
getStatementType query
    | "select" `isPrefixOf` lowerQuery = if "show" `isPrefixOf` lowerQuery then ShowTable else Select
    | "insert" `isPrefixOf` lowerQuery = Insert
    | "update" `isPrefixOf` lowerQuery = Update
    | "delete" `isPrefixOf` lowerQuery = Delete
    | "create table" `isPrefixOf` lowerQuery = CreateTable
    | "drop table" `isPrefixOf` lowerQuery = DropTable
    | "show tables" `Data.List.isInfixOf` lowerQuery = ShowTables
    | "show table" `isPrefixOf` lowerQuery = ShowTable
    | otherwise = InvalidStatement
  where
    lowerQuery = map toLower query

convertValue :: Y.Value -> DataFrame.Value
convertValue val = case val of
    Y.String s -> DataFrame.StringValue (T.unpack s)
    Y.Number n -> DataFrame.IntegerValue (round n)
    Y.Bool b -> DataFrame.BoolValue b
    Y.Null -> DataFrame.NullValue
    _ -> error "Unsupported value type"

serializeTableToYAML :: SerializedTable -> String
serializeTableToYAML st =
    "tableName: " Data.List.++ tableName st Data.List.++ "\n" Data.List.++
    "columns:\n" Data.List.++ Data.List.concatMap serializeColumn (columns st) Data.List.++
    "rows:\n" Data.List.++ Data.List.concatMap serializeRow (rows st)
  where
    serializeColumn :: SerializedColumn -> String
    serializeColumn col =
        "- name: " Data.List.++ name col Data.List.++ "\n" Data.List.++
        "  dataType: " Data.List.++ dataType col Data.List.++ "\n"

    serializeRow :: [Y.Value] -> String
    serializeRow row = "- [" Data.List.++ Data.List.intercalate ", " (Data.List.map serializeValue row) Data.List.++ "]\n"

    serializeValue :: Y.Value -> String
    serializeValue val =
        case val of
            Y.String s -> T.unpack s
            Y.Number n -> show (round n :: Int)
            Y.Bool b   -> Data.List.map Data.Char.toLower (show b)
            Y.Null     -> "null"
            _          -> "ERROR"

dataFrameToSerializedTable :: (TableName, DataFrame) -> SerializedTable
dataFrameToSerializedTable (tblName, DataFrame columns rows) =
    SerializedTable {
        tableName = tblName,
        columns = Data.List.map convertColumnToSerializedColumn columns,
        rows = Data.List.map convertRow rows
    }
  where
    convertColumnToSerializedColumn :: Column -> SerializedColumn
    convertColumnToSerializedColumn (Column name columnType) =
        SerializedColumn {
            name = name,
            dataType = convertColumnTypeToString columnType
        }

    convertColumnTypeToString :: ColumnType -> String
    convertColumnTypeToString IntegerType = "integer"
    convertColumnTypeToString StringType = "string"
    convertColumnTypeToString BoolType = "boolean"

    convertRow :: Row -> [Y.Value]
    convertRow = Data.List.map convertValueToYValue

    convertValueToYValue :: Value -> Y.Value
    convertValueToYValue (IntegerValue n) = Y.Number (fromIntegral n)
    convertValueToYValue (StringValue s) = Y.String (T.pack s)
    convertValueToYValue (BoolValue b) = Y.Bool b
    convertValueToYValue NullValue = Y.Null


-- =======================================
-- Statement validation


validateStatement stmt tables = case stmt of
  SelectAll tableNames whereClause sortClause ->
    returnError $ validateSelectAll tableNames whereClause sortClause tables
  SelectColumns tableNames cols whereClause sortClause ->
    returnError $ validateSelectColumns tableNames (Just cols) whereClause sortClause tables
  SelectAggregate tableNames cols whereClause sortClause ->
    returnError $ validateSelectAggregate tableNames cols whereClause sortClause tables
  InsertStatement tableName cols vals ->
    returnError $ validateTableAndColumns [tableName] cols tables &&
      all (\(column, value) -> selectColumnMatchesValue column tables value) (zip (fromMaybe [] cols) vals)
  UpdateStatement tableName cols vals whereClause ->
    returnError $ validateTableAndColumns [tableName] (Just cols) tables &&
      validateWhereClause whereClause tables &&
      all (\(column, value) -> selectColumnMatchesValue column tables value) (zip cols vals)
  DeleteStatement tableName whereClause ->
    returnError $ tableName `elem` map fst tables && validateWhereClause whereClause tables
  ShowTablesStatement ->
    returnError True
  CreateTableStatement tableName cols ->
    returnError True
  DropTableStatement tableName ->
    validateDropTableStatement (DropTableStatement tableName) tables
  ShowTableStatement tableName ->
    returnError $ elem tableName $ map fst tables
  Invalid err ->
    (False, err)

returnError :: Bool -> (Bool, String)
returnError bool = (bool, "Non existent columns or tables in statement or values don't match column")

validateSelectAll :: [TableName] -> Maybe WhereClause -> Maybe SortClause -> [(TableName, DataFrame)] -> Bool
validateSelectAll tableNames whereClause sortClause tables =
  all (`elem` map fst tables) tableNames &&
  validateWhereClause whereClause tables &&
  validateSortClause sortClause tables

validateSelectColumns :: [TableName] -> Maybe [SelectColumn] -> Maybe WhereClause -> Maybe SortClause -> [(TableName, DataFrame)] -> Bool
validateSelectColumns tableNames cols whereClause sortClause tables =
  validateTableAndColumns tableNames cols tables &&
  validateWhereClause whereClause tables &&
  validateSortClause sortClause tables

validateSelectAggregate :: [TableName] -> [SelectColumn] -> Maybe WhereClause -> Maybe SortClause -> [(TableName, DataFrame)] -> Bool
validateSelectAggregate tableNames cols whereClause sortClause tables =
  validateTableAndColumns tableNames (Just cols) tables &&
  validateWhereClause whereClause tables &&
  validateSortClause sortClause tables

validateSortClause :: Maybe SortClause -> [(TableName, DataFrame)] -> Bool
validateSortClause (Just (ColumnSort selectedColumns _)) tables =
  all (\col -> case col of
                 TableColumn tName cName -> columnExistsInTable (TableColumn tName cName) (lookup tName tables)
                 _ -> False) selectedColumns
validateSortClause Nothing _ = True

columnExistsInTable :: SelectColumn -> Maybe DataFrame -> Bool
columnExistsInTable (TableColumn _ colName) (Just (DataFrame cols _)) =
  any (\(Column name _) -> name == colName) cols
columnExistsInTable _ _ = False

validateDropTableStatement :: ParsedStatement -> [(TableName, DataFrame)] -> (Bool, ErrorMessage)
validateDropTableStatement (DropTableStatement tableName) tables =
    if tableName `elem` map fst tables
    then (True, "")
    else (False, "Table does not exist: " ++ tableName)
validateDropTableStatement _ _ = (False, "Invalid statement type for DropTable validation")


validateWhereClause :: Maybe WhereClause -> [(TableName, DataFrame)] -> Bool
validateWhereClause clause tables = case clause of
  Just cClause -> validateExistingClause cClause tables
  Nothing -> True

validateExistingClause :: WhereClause -> [(TableName, DataFrame)] -> Bool
validateExistingClause (IsValueBool _ tableName columnName ) tables = tablesExist [tableName] tables && colInDf && columnIsBool
  where
    df = fromMaybe (DataFrame [] []) (Data.List.lookup tableName tables)
    colInDf = columnExistsInDataFrame (TableColumn tableName columnName) df
    columnIsBool = getColumnType (getColumnByName columnName (columnsList df)) == BoolType

validateExistingClause (Conditions conditions) tables = Data.List.all validCondition conditions
  where
    validCondition :: Condition -> Bool
    validCondition (Equals column val) = columnExists column tables && selectColumnMatchesValue column tables (getValueFromConditionValue val)
    validCondition (LessThan column val) = columnExists column tables && selectColumnMatchesValue column tables (getValueFromConditionValue val)
    validCondition (GreaterThan column val) = columnExists column tables && selectColumnMatchesValue column tables (getValueFromConditionValue val)
    validCondition (LessthanOrEqual column val) = columnExists column tables && selectColumnMatchesValue column tables (getValueFromConditionValue val)
    validCondition (GreaterThanOrEqual column val) = columnExists column tables && selectColumnMatchesValue column tables (getValueFromConditionValue val)
    validCondition (NotEqual column val) = columnExists column tables && selectColumnMatchesValue column tables (getValueFromConditionValue val)
    validCondition (ColumnValuesEqual col1 col2) = columnExists col1 tables && columnExists col2 tables && columnTypesMatch col1 col2

    columnExists :: SelectColumn -> [(TableName, DataFrame)] -> Bool
    columnExists column tbs = maybe False (columnsExistInTable [column]) (Data.List.lookup (getTableNameFromColumn column) tbs)

    columnTypesMatch :: SelectColumn -> SelectColumn -> Bool
    columnTypesMatch (TableColumn tName1 cName1) (TableColumn tName2 cName2) = table1ColumnType == table2ColumnType
      where
        table1ColumnType = getColumnType $ getColumnByName cName1 $ getTableColumns tables tName1
        table2ColumnType = getColumnType $ getColumnByName cName2 $ getTableColumns tables tName2
    columnTypesMatch _ _ = False

selectColumnMatchesValue :: SelectColumn -> [(TableName, DataFrame)] -> Value -> Bool
selectColumnMatchesValue col@(TableColumn tableName columnName) tables val = tableExists && isMatchingValue
  where
    tableExists = validateTableAndColumns [tableName] (Just [col]) tables
    columns = getTableColumns tables tableName
    column = getColumnByName columnName columns
    isMatchingValue = columnMatchesValue column val

selectColumnMatchesValue _ _ _ = False

columnsList :: DataFrame -> [Column]
columnsList (DataFrame cols _) = cols

getColumnByName :: String -> [Column] -> Column
getColumnByName name cols = fromMaybe (Column "notfound" BoolType) (Data.List.find (\(Column colName _) -> colName == name) cols)


getValueFromConditionValue :: ConditionValue -> Value
getValueFromConditionValue (StrValue str) = StringValue str
getValueFromConditionValue (IntValue num) = IntegerValue num

columnMatchesValue :: Column -> Value -> Bool
columnMatchesValue (Column _ t) value = isNothing maybeValueType || isJust maybeValueType && fromMaybe IntegerType maybeValueType == t
  where
    maybeValueType = getColumnTypeFromValueType value

getTableNameFromColumn :: SelectColumn -> TableName
getTableNameFromColumn (TableColumn tname _) = tname
getTableNameFromColumn (Avg tname _) = tname
getTableNameFromColumn (Max tname _) = tname
getTableNameFromColumn Now = "NO TABLE"

getColumnTypeFromValueType :: Value -> Maybe ColumnType
getColumnTypeFromValueType (IntegerValue _) = Just IntegerType
getColumnTypeFromValueType (StringValue _) = Just StringType
getColumnTypeFromValueType (BoolValue _) = Just BoolType
getColumnTypeFromValueType NullValue = Nothing

getColumnType :: Column -> ColumnType
getColumnType (Column _ columnType) = columnType

tablesExist :: [TableName] -> [(TableName, DataFrame)] -> Bool
tablesExist tables databaseTables = Data.List.all (`Data.List.elem` tablesInDatabase) tables
  where tablesInDatabase = Data.List.map fst databaseTables

validateTableAndColumns :: [TableName] -> Maybe [SelectColumn] -> [(TableName, DataFrame)] -> Bool
validateTableAndColumns tableNames cols tables = isJust cols && Data.List.all tableAndColumnsExist tableNames
  where
    justCols = fromMaybe [] cols
    tableAndColumnsExist :: TableName -> Bool
    tableAndColumnsExist tableName = maybe False (columnsExistInTable (filter (selectColumnIsFromTable tableName) justCols)) (Data.List.lookup tableName tables)

    selectColumnIsFromTable :: TableName -> SelectColumn -> Bool
    selectColumnIsFromTable tableName (TableColumn tName _) = tName == tableName
    selectColumnIsFromTable tableName (Avg tName _) = tName == tableName
    selectColumnIsFromTable tableName (Max tName _) = tName == tableName
    selectColumnIsFromTable _ Now = True


columnsExistInTable :: [SelectColumn] -> DataFrame -> Bool
columnsExistInTable columns df = Data.List.all (`columnExistsInDataFrame` df) columns

columnExistsInDataFrame :: SelectColumn -> DataFrame -> Bool
columnExistsInDataFrame (TableColumn _ colName) (DataFrame cols _) =
  Data.List.any (\(Column name _) -> name == colName) cols

columnExistsInDataFrame (Avg _ colName) (DataFrame cols _) =
  Data.List.any (\(Column name t) -> name == colName && t == IntegerType) cols

columnExistsInDataFrame (Max _ colName) (DataFrame cols _) =
  Data.List.any (\(Column name _) -> name == colName) cols

columnExistsInDataFrame Now _ = True


-- ============================
-- End of StatementValidation


getSelectedColumns :: Lib4.ParsedStatement -> [(Lib4.TableName, DataFrame)] -> [Column]
getSelectedColumns stmt tbls = case stmt of
    Lib4.SelectAll tableNames _ _ -> Data.List.concatMap (getTableColumns tbls) tableNames
    Lib4.SelectColumns _ selectedColumns _ _ -> case getFilteredDataFrame (map snd tbls) selectedColumns of
      Right df -> getDataFrameColumns df
      Left _ -> []  -- mapMaybe (findColumn tbls) selectedColumns
    Lib4.SelectAggregate _ selectedColumns _ _ -> mapMaybe (findColumn tbls) selectedColumns
    _ -> []

getTableColumns :: [(Lib4.TableName, DataFrame)] -> Lib4.TableName -> [Column]
getTableColumns tbls tableName = case Data.List.lookup tableName tbls of
    Just (DataFrame columns _) -> columns
    Nothing -> []

findColumn :: [(Lib4.TableName, DataFrame)] -> Lib4.SelectColumn -> Maybe Column
findColumn tbls (Lib4.TableColumn tblName colName) = findColumnInTable tbls tblName colName
findColumn tbls (Lib4.Max tblName colName) = findColumnInTable tbls tblName colName
findColumn tbls (Lib4.Avg tblName colName) = findColumnInTable tbls tblName colName
findColumn _ _ = Nothing

findColumnInTable :: [(Lib4.TableName, DataFrame)] -> Lib4.TableName -> String -> Maybe Column
findColumnInTable tbls tblName colName =
    case Data.List.lookup tblName tbls of
        Just (DataFrame columns _) -> Data.List.find (\(Column name _) -> name == colName) columns
        Nothing -> Nothing


-- SQL PARSER


runParser :: ParsedStatementT a -> [String] -> Either ErrorMessage a
runParser parser input =
    let res = runState (runEitherT parser) input
    in case res of
        (Left err, _) -> Left err
        (Right value, _) -> Right value

parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement input
  | Data.List.last input /= ';' = Left "Missing semicolon at end of statement"
  | otherwise = case eitherResult of
    Left er -> Left er
    Right st -> Right st
  where
    cleanedInput = Data.List.init input
    wordsInput = parseSemiCaseSensitive cleanedInput
    parseResult = mapStatementType
    eitherResult = runParser parseResult wordsInput

mapStatementType :: ParsedStatementT ParsedStatement
mapStatementType = do
  statement <- lift get
  statementType <- guessStatementType
  case statementType of
      Select -> do
        lift $ put $ drop 1 statement
        parseSelect
      Insert -> parseInsert
      Delete -> parseDelete
      Update -> parseUpdate
      ShowTable -> parseShowTable
      ShowTables -> parseShowTables
      DropTable -> parseDropTable
      CreateTable -> parseCreateTable
      InvalidStatement -> EitherT $ return $ Left "Invalid statement type"

guessStatementType :: ParsedStatementT StatementType
guessStatementType = do
  statement <- lift get
  case statement of
    ("select" : _) -> return Select
    ("insert" : "into" : _) -> return Insert
    ("delete" : "from" : _) -> return Delete
    ("update" : _) -> return Update
    ("show" : "table" : _) -> return ShowTable
    ("show" : "tables" : _) -> return ShowTables
    ["drop", "table", _] -> return DropTable
    ("create" : _) -> return CreateTable
    _ -> EitherT $ return $ Left "Sql statement does not resemble any sort of implemented statement"

parseInsert :: ParsedStatementT ParsedStatement
parseInsert = do
  tableName <- getInsertTableName
  getInsertColumns tableName

parseDropTable :: ParsedStatementT ParsedStatement
parseDropTable = do
  statement <- lift get
  return $ DropTableStatement $ last statement

parseCreateTable :: ParsedStatementT ParsedStatement
parseCreateTable = do
  tableName <- getTableNameFromCreate
  CreateTableStatement tableName <$> getColumnsFromCreate

getTableNameFromCreate :: ParsedStatementT TableName
getTableNameFromCreate = do
  statement <- lift get
  let (_, tableAndAfter) = break (== "table") statement
  let tableName = takeWhile (\element -> head element /= '(') (drop 1 tableAndAfter)
  case tableName of
    [tname] -> return tname
    _ -> EitherT $ return $ Left "Error parsing create table statement table name"

getColumnsFromCreate :: ParsedStatementT [Column]
getColumnsFromCreate = do
  statement <- lift get
  let (_, tableAndAfter) = break (== "table") statement
  let columns = drop 2 tableAndAfter
  cleanedCreate <- cleanCreate columns
  splitCreateColumns <- splitByTwo cleanedCreate
  getCreateColumns splitCreateColumns

splitByTwo :: [String] -> ParsedStatementT [(String, String)]
splitByTwo [one, two] = return [(one, two)]
splitByTwo (one : two : xs) = do
  let initial = (one, two)
  rest <- splitByTwo xs
  return $ initial : rest
splitByTwo _ = EitherT $ return $ Left "Failed to combine column name and value type"

getCreateColumns :: [(String, String)] -> ParsedStatementT [Column]
getCreateColumns [(colName, valueType)] = do
  parsedColumnType <- parseColumnType valueType
  return [Column colName parsedColumnType]

getCreateColumns (x : xs) = do
  initColumn <- getCreateColumns [x]
  rest <- getCreateColumns xs
  return $ initColumn ++ rest

getCreateColumns [] = EitherT $ return $ Left "Failed to parse columns in create statement"

parseColumnType :: String -> ParsedStatementT ColumnType
parseColumnType str 
  | map toLower str == "int" = return IntegerType
  | map toLower str == "bool" = return BoolType
  | "varchar(" `isPrefixOf` map toLower str && ")" `isSuffixOf` map toLower str = return StringType
  | otherwise = EitherT $ return $ Left $ "Failed to parse column type" ++ str

cleanCreate :: [String] -> ParsedStatementT [String]
cleanCreate statement = do
  striped <- stripCreate statement
  removeCommas striped

stripCreate :: [String] -> ParsedStatementT [String]
stripCreate statement =
  if length statement > 1 && head (head statement) == '(' && last (last statement) == ')'
    then return $ [drop 1 (head statement)] ++ drop 1 (init statement) ++ [init (last statement)]
    else EitherT $ return $ Left "Create statement does not have correct brackets"

removeCommas :: [String] -> ParsedStatementT [String]
removeCommas statement = do
  let isValidLength = even $ length statement
  if isValidLength
    then dropCommas statement 1
    else EitherT $ return $ Left "Create statement does not have correct commas"

dropCommas :: [String] -> Int -> ParsedStatementT [String]
dropCommas [val] _ =
  if last val == ','
    then
      EitherT $ return $ Left "Too many commas"
    else
      return [val]
dropCommas (x : xs) a = do
  initValue <- dropCommas' x a
  rest <- dropCommas xs (a + 1)
  return $ initValue : rest

dropCommas [] _ = EitherT $ return $ Left "No empty tables allowed"

dropCommas' :: String -> Int -> ParsedStatementT String
dropCommas' val a
  | even a = if last val == ','
        then return $ init val
        else EitherT $ return $ Left "Missing comma"
  | last val == ',' = EitherT $ return $ Left "Missing comma"
  | otherwise = return val

getInsertTableName :: ParsedStatementT TableName
getInsertTableName = do
  statement <- lift get
  let (_, intoAndAfter) = Data.List.break (== "into") statement
  let tableToInsertInto = Data.List.drop 1 intoAndAfter
  if not (Data.List.null tableToInsertInto) && isValidTable (Data.List.head tableToInsertInto)
      then return $ Data.List.head tableToInsertInto
      else EitherT $ return $ Left "Update statement table name does not meet requirements. Maybe ilegal characters were used?"

getInsertColumns :: TableName -> ParsedStatementT ParsedStatement
getInsertColumns tableName = do
  statement <- lift get
  let (_, intoAndAfter) = Data.List.break (== "into") statement
  let (intoToValues, valuesAndAfter) = Data.List.break (== "values") intoAndAfter
  cleanedValues <- cleanInsertHeadAndTail (Data.List.drop 1 valuesAndAfter)
  columnNames <- getColumnNames $ Data.List.drop 2 intoToValues
  columnValues <- getInsertColumnValues cleanedValues
  columns <-
    if Data.List.length columnNames /= Data.List.length columnValues
      then EitherT $ return $ Left "Column count does not match value count in insert statement"
      else return $ Data.List.map (TableColumn tableName) columnNames
  getInsertStatement tableName columns columnValues

getInsertStatement :: TableName -> [SelectColumn] -> [Value] -> ParsedStatementT ParsedStatement
getInsertStatement tableName columns values = return $ InsertStatement tableName (Just columns)  values

-- valueToString :: Value -> String
-- valueToString (IntegerValue i) = show i
-- valueToString (StringValue s) = s
-- valueToString (BoolValue b) = show b
-- valueToString NullValue = "null"

getInsertColumnValues :: [String] -> ParsedStatementT [Value]
getInsertColumnValues [value] = do
  val <- getValueFromString value
  return [val]

getInsertColumnValues (value : xs) = do
  cleanedString <- if Data.List.last value == ',' then return $ Data.List.init value else EitherT $ return $ Left "Failed to parse INSERT values"
  val <- getValueFromString cleanedString
  rest <- getInsertColumnValues xs
  return $ val : rest

getInsertColumnValues _ = EitherT $ return $ Left "Error parsing insert statement values"

getColumnNames :: [String] -> ParsedStatementT [String]
getColumnNames rawNames = do
  cleanedHeadAndTail <- cleanInsertHeadAndTail rawNames
  cleanInsertCommas cleanedHeadAndTail

cleanInsertHeadAndTail :: [String] -> ParsedStatementT [String]
cleanInsertHeadAndTail input =
  if not (Data.List.null input) && Data.List.head (Data.List.head input) == '(' && Data.List.last (Data.List.last input) == ')'
    then
      if Data.List.length input == 1
        then return [Data.List.drop 1 (Data.List.init (Data.List.head input))]
        else return $ [Data.List.drop 1 (Data.List.head input)] Data.List.++ Data.List.init (Data.List.drop 1 input) Data.List.++ [Data.List.init (Data.List.last input)]
    else EitherT $ return $ Left "formating of insert statement does not meet requirements. Most likely missing `(` or `)`"

cleanInsertCommas :: [String] -> ParsedStatementT [String]
cleanInsertCommas [column] =
  if isValidColumnWithoutAb column
    then return [column]
    else EitherT $ return $ Left $ "Column" Data.List.++ " `" Data.List.++ column Data.List.++ "` " Data.List.++ "contains ilegal characters"

cleanInsertCommas (column : xs) = do
  currentColumn <-
    if Data.List.last column == ',' && isValidColumnWithoutAb column
      then return $ Data.List.init column
      else EitherT $ return $ Left "Missing comma or invalid column name for insert statement"
  rest <- cleanInsertCommas xs
  return $ currentColumn : rest

cleanInsertCommas _ = EitherT $ return $ Left "Unknown error parsing columns in insert statement"

parseDelete :: ParsedStatementT ParsedStatement
parseDelete = do
  statement <- lift get
  tableName <- getTableNameFromDelete
  let (_, fromWhere) = Data.List.break (== "where") statement
  whereClause <- statementClause' fromWhere tableName
  getDeleteStatement tableName whereClause

getDeleteStatement :: TableName -> Maybe WhereClause -> ParsedStatementT ParsedStatement
getDeleteStatement tableName whereClause = return $ DeleteStatement tableName whereClause

getTableNameFromDelete :: ParsedStatementT TableName
getTableNameFromDelete = do
  statement <- lift get
  let (_, fromAndElse) = Data.List.break (== "from") statement
  let (fromToWhere, _) = Data.List.break (== "where") fromAndElse
  if Data.List.length fromToWhere /= 2 || not (isValidTable (Data.List.last fromToWhere))
      then EitherT $ return $ Left $ "Invalid delete statement table name: " Data.List.++ Data.List.last fromToWhere
      else return $ Data.List.last fromToWhere


parseUpdate :: ParsedStatementT ParsedStatement
parseUpdate = do
  statement <- lift get
  let (_, afterWhere) = Data.List.break (== "where") statement
  tableName <- getUpdateTableName
  columnAndNewValueString <- getColumnAndNewValueStringsForUpdate
  selectedColumns <- getSelectedColumnsForUpdate columnAndNewValueString tableName
  updateValues <- getValuesToUpdate columnAndNewValueString
  whereClause <- statementClause' afterWhere tableName
  getUpdateStatement tableName selectedColumns updateValues whereClause

-- Why did I use `do` here? Well... Thats a good question that I don't know the answer to
getUpdateTableName :: ParsedStatementT TableName
getUpdateTableName = do
  statement <- lift get
  let (beforeSet, _) = Data.List.break (== "set") statement
  let tableToUpdate = Data.List.drop 1 beforeSet
  if isUpdateTableNameValid tableToUpdate
      then return $ Data.List.head tableToUpdate
      else EitherT $ return $ Left "Update statement table name does not meet requirements. Maybe ilegal characters were used?"

isUpdateTableNameValid :: [String] -> Bool
isUpdateTableNameValid table = Data.List.length table == 1 && isValidTable (Data.List.head table)

getColumnAndNewValueStringsForUpdate :: ParsedStatementT [String]
getColumnAndNewValueStringsForUpdate = do
  updateStatement <- lift get
  let (_, setAndAfter) = Data.List.break (== "set") updateStatement
  let (fromSetToWhere, _) = Data.List.break (== "where") setAndAfter
  let cleanedFromSetToWhere = cleanLastComma fromSetToWhere
  return $ Data.List.drop 1 cleanedFromSetToWhere

cleanLastComma :: [String] -> [String]
cleanLastComma = Data.List.map (\str -> if Data.List.last str == ',' then Data.List.init str else str)

getSelectedColumnsForUpdate :: [String] -> TableName -> ParsedStatementT [SelectColumn]
getSelectedColumnsForUpdate [val1, op, val2] tableName = do
  baseColumn <- getValidUpdateColumn val1 op val2 tableName
  return [baseColumn]

getSelectedColumnsForUpdate (val1 : op : val2 : xs) tableName = do
  baseColumn <- getValidUpdateColumn val1 op val2 tableName
  rest <- getSelectedColumnsForUpdate xs tableName
  return $ rest Data.List.++ [baseColumn]

getSelectedColumnsForUpdate _ _ = EitherT $ return $ Left "Invalid column formating for UPDATE statement"

getValidUpdateColumn :: String -> String -> String -> TableName -> ParsedStatementT SelectColumn
getValidUpdateColumn val1 op val2 tableName
  | op == "=" && checkIfDoesNotContainSpecialSymbols val1 && (isNumber val2 || val2 == "true" || val2 == "false" || "'" `Data.List.isPrefixOf` val2 && "'" `Data.List.isSuffixOf` val2) = return $ TableColumn tableName val1
  | otherwise = EitherT $ return $ Left "Unable to update table do to bad update statement. Failed to parse new column values"

checkIfDoesNotContainSpecialSymbols :: String -> Bool
checkIfDoesNotContainSpecialSymbols val1 = Data.List.notElem '\'' val1 && Data.List.notElem '.' val1 && Data.List.notElem '(' val1 && Data.List.notElem ')' val1

getValuesToUpdate :: [String] -> ParsedStatementT [Value]
getValuesToUpdate [val1, op, val2] = do
  baseColumn <- getValidUpdateValue val1 op val2
  return [baseColumn]

getValuesToUpdate (val1 : op : val2 : xs) = do
  baseColumn <- getValidUpdateValue val1 op val2
  rest <- getValuesToUpdate xs
  return $ rest Data.List.++ [baseColumn]

getValuesToUpdate _  = EitherT $ return $ Left "Invalid column formating for UPDATE statement"

getValidUpdateValue :: String -> String -> String -> ParsedStatementT Value
getValidUpdateValue val1 op val2
  | op == "=" && checkIfDoesNotContainSpecialSymbols val1 && (isNumber val2 || val2 == "true" || val2 == "false" || "'" `Data.List.isPrefixOf` val2 && "'" `Data.List.isSuffixOf` val2) = getValueFromString val2
  | otherwise = EitherT $ return $ Left "Unable to update table do to bad update statement. Failed to parse columns to be updated"

getValueFromString :: String -> ParsedStatementT Value
getValueFromString valueString
  | valueString == "true" = return $ BoolValue True
  | valueString == "false" = return $ BoolValue False
  | isNumber valueString = return $ IntegerValue (read valueString :: Integer)
  | "'" `Data.List.isPrefixOf` valueString && "'" `Data.List.isSuffixOf` valueString = return $ StringValue $ Data.List.drop 1 (Data.List.init valueString)
  | otherwise = EitherT $ return $ Left "Failed to parse UPDATE statement value. Only string, integer and bool values allowed"

getUpdateStatement :: TableName -> [SelectColumn] -> Row -> Maybe WhereClause -> ParsedStatementT ParsedStatement
getUpdateStatement tableName selectedColumns rows whereClause = return $ UpdateStatement tableName selectedColumns rows whereClause


parseShowTables :: ParsedStatementT ParsedStatement
parseShowTables = do
  statement <- lift get
  case statement of
    ["show", "tables"] -> return ShowTablesStatement
    _ -> EitherT $ return $ Left "Failed to parse SHOW TABLES statement"

parseShowTable :: ParsedStatementT ParsedStatement
parseShowTable = do
  statement <- lift get
  case statement of
    ["show", "table", tableName] ->
      if isValidTable tableName
        then return $ ShowTableStatement tableName
        else EitherT $ return $ Left $ "SHOW TABLE table name contains ilegal characters"Data.List.++ " " Data.List.++ tableName
    _ -> EitherT $ return $ Left "Failed to parse SHOW TABLE statement"

isValidTable :: String -> Bool
isValidTable originalName
  | "'" `Data.List.isPrefixOf` originalName || "'" `Data.List.isSuffixOf` originalName || "avg(" `Data.List.isPrefixOf` originalName || "max(" `Data.List.isPrefixOf` originalName || ")" `Data.List.isSuffixOf` originalName = False
  | otherwise = True

parseSelect :: ParsedStatementT ParsedStatement
parseSelect = do
  statement <- lift get
  tableNamesAndAb <- getTableNamesAndAb
  (beforeWhere, afterWhere) <- return $ Data.List.break (== "where") statement
  (beforeFrom, _) <- return $ Data.List.break (== "from") beforeWhere
  whereClause <- statementClause afterWhere tableNamesAndAb
  selectType <- getSelectType beforeFrom
  let tableNames = Data.List.map fst tableNamesAndAb
  sort <- getSort tableNamesAndAb
  case selectType of
    Aggregate -> do
      aggregateColumns <- getAggregateColumns beforeFrom tableNamesAndAb
      parseAggregate tableNames whereClause aggregateColumns sort
    ColumnsAndTime -> do
      columnsWithTableAb <- getColumnWithTableAb tableNamesAndAb
      parseColumnsSelect tableNames columnsWithTableAb whereClause sort
    AllColumns -> parseAllColumns tableNames whereClause sort

castEither :: a -> Either ErrorMessage a -> a
castEither defaultValue eitherValue = case eitherValue of
  Right val -> val
  Left _ -> defaultValue

getSort :: [(TableName, String)] -> ParsedStatementT (Maybe SortClause)
getSort tables = do
  statement <- lift get
  let (_, orderAndAfter) = break (=="order") statement
  let (nothing, byAndAfter) = break (=="by") orderAndAfter
  let hasSortClause = not $ null orderAndAfter
  let hasValidSortClause = hasSortClause && concat nothing == "order" && isValidSortClause (drop 1 byAndAfter) tables
  if hasSortClause
    then
      if hasValidSortClause
        then parseSortClause tables (drop 1 byAndAfter)
        else EitherT $ return $ Left "Invalid sort clause"
    else return Nothing

parseSortClause :: [(TableName, String)] -> [String] -> ParsedStatementT (Maybe SortClause)
parseSortClause tables columns = do
  parsedColumns <- parseSortColumns (init columns) tables
  order <- getSortOrder (last columns)
  return $ Just $ ColumnSort parsedColumns (Just order)

parseSortColumns :: [String] -> [(TableName, String)] -> ParsedStatementT [SelectColumn]
parseSortColumns columns tables = return parsedColumns
  where
    abAndColumn = map (break (=='.')) columns
    parsedColumns = getColumns abAndColumn

    getColumns :: [(String, String)] -> [SelectColumn]
    getColumns [a] = [col]
      where
        tableName = getFstTupleElemBySndElemInList a tables
        col = case tableName of
          Right tname -> TableColumn tname (drop 1 (snd a))
          Left _ -> TableColumn "" ""
    getColumns (x : xs) = cols
      where
        fstCol = getColumns [x]
        cols = fstCol ++ getColumns xs
    getColumns [] = []
getSortOrder :: String -> ParsedStatementT SortOrder
getSortOrder order = case order of
  "asc" -> return Asc
  "desc" -> return Desc
  _ -> EitherT $ return $ Left "Error parsing Order By"

isValidSortClause :: [String] -> [(TableName, String)] -> Bool
isValidSortClause clause tables = lastIsOrder && hasAllValidColumns (init clause) && validTableAb
  where
    lastIsOrder = not (null clause) && (last clause == "asc" || last clause == "desc")
    tableAbs = map snd tables
    abAndColumn = map (break (=='.')) (init clause)
    columnAbs = map fst abAndColumn
    validTableAb = all (`elem` tableAbs) columnAbs

getAggregateColumns :: [String] -> [(TableName, String)] -> ParsedStatementT [SelectColumn]
getAggregateColumns [baseColumn] tableNames = do
  let stripedColumn = if Data.List.last baseColumn == ',' then Data.List.init baseColumn else baseColumn
  if stripedColumn == "now()"
    then return [Now]
    else do
      parsedAggregate <- parseAggregateColumn stripedColumn tableNames
      return [parsedAggregate]
getAggregateColumns (baseColumn : xs) tableNames = do
  let stripedColumn = if Data.List.last baseColumn == ',' then Data.List.init baseColumn else baseColumn
  if stripedColumn == "now()"
    then do
      rest <- getAggregateColumns xs tableNames
      return $ Now : rest
    else do
      baseAggregate <- parseAggregateColumn baseColumn tableNames
      rest <- getAggregateColumns xs tableNames
      return $ baseAggregate : rest
getAggregateColumns _ _ = EitherT $ return $ Left "Error parsing aggregate columns"

parseAggregateColumn :: String -> [(TableName, String)] -> ParsedStatementT SelectColumn
parseAggregateColumn column tableNames
  | isValid && "avg(" `Data.List.isPrefixOf` dropedCommaColumn && ")" `Data.List.isSuffixOf` dropedCommaColumn = return $ Avg tableName columnName
  | isValid && "max(" `Data.List.isPrefixOf` dropedCommaColumn && ")" `Data.List.isSuffixOf` dropedCommaColumn = return $ Max tableName columnName
  | dropedCommaColumn == "now()" = return Now
  | otherwise = EitherT $ return $ Left $ "Failed to parse aggregate column " Data.List.++ column
    where
      dropedCommaColumn = if Data.List.last column == ',' then Data.List.init column else column
      removedAggregate = if Data.List.last column == ',' then Data.List.init (Data.List.drop 4 (Data.List.init column)) else Data.List.drop 4 (Data.List.init column)
      -- checked in function that calls this function so should be fine if code doesn't change (for now)
      [tableAb, columnName] = wordsWhen (== '.') removedAggregate
      isValid = findIfTupleWithSndElemEqualToExists tableAb tableNames
      tableName = castEither "Upsi" $ getFstTupleElemBySndElemInList (tableAb, columnName) tableNames


findIfTupleWithSndElemEqualToExists :: Eq a => a -> [(b, a)] -> Bool
findIfTupleWithSndElemEqualToExists _ [] = False;
findIfTupleWithSndElemEqualToExists val [(_, sndVal)] = val == sndVal
findIfTupleWithSndElemEqualToExists val ((_, sndVal) : xs) = val == sndVal || findIfTupleWithSndElemEqualToExists val xs


getTableNamesAndAb :: ParsedStatementT [(TableName, String)]
getTableNamesAndAb = do
  statement <- lift get
  let
    (_, afterFrom) = Data.List.break (== "from") statement
    (bforeOrder, _) = Data.List.break (== "order") afterFrom
    (tables, _) = Data.List.break (== "where") bforeOrder
    dropedTable = Data.List.drop 1 tables
    len = Data.List.length dropedTable
    abbs = Data.List.map snd (fst (Data.List.partition (odd . fst) (Data.List.zip [0 .. ] dropedTable)))
    tuples = splitIntoTuples dropedTable

    tuplesWithoutCommaAtEndOfAbb = case tuples of
      Right tupleList -> Right $ Data.List.map (\(tableName, tableAb) -> if Data.List.last tableAb == ',' then (tableName, Data.List.init tableAb) else (tableName, tableAb)) tupleList
      Left err -> Left err
    names
      | even len && len > 0 && valuesListLike abbs = tuplesWithoutCommaAtEndOfAbb
      | otherwise = Left "Invalid table formating in statement. Maybe table abbreviation was not provided?"
  case names of
    Right n -> return n
    Left err -> EitherT $ return $ Left err

valuesListLike :: [String] -> Bool
valuesListLike [val] = Data.List.last val /= ','
valuesListLike (x : xs) = Data.List.last x == ',' && valuesListLike xs
valuesListLike [] = False

getColumnWithTableAb :: [(TableName, String)] -> ParsedStatementT [(String, String)]
getColumnWithTableAb tableNames = do
  statement <- lift get
  (beforeFrom, _) <- return $ Data.List.break (== "from") statement
  let columnAndAbList = getListOfTableAbAndColumn beforeFrom
  let isValidColumnNames = Data.List.all isValidColumn beforeFrom
  if Data.List.null columnAndAbList || odd (Data.List.length columnAndAbList) || not isValidColumnNames
    then EitherT $ return $ Left "error parsing columns. Maybe table name abbreviation was not provided?"
    else do
      let splitList = splitIntoTuples columnAndAbList
      case splitList of
        Right sl -> getCorrectTableAndColumns sl tableNames
        Left err -> EitherT $ return $ Left err

getCorrectTableAndColumns :: [(String, String)] -> [(String, String)] -> ParsedStatementT [(String, String)]
getCorrectTableAndColumns [columnAndTableAb] tableNames = if columnAndTableAb == ("now()", "now()")
  then
    return [("now()", "now()")]
  else do
    let tableName = getFstTupleElemBySndElemInList columnAndTableAb tableNames
    case tableName of
      Right tn -> return [(tn, snd columnAndTableAb)]
      Left err -> EitherT $ return $ Left err

getCorrectTableAndColumns (columnAndTableAb : xs) tableNames = if columnAndTableAb == ("now()", "now()")
then do
  rest <- getCorrectTableAndColumns xs tableNames
  return $ ("now()", "now()") : rest
else do
  let tableExists = findIfTupleWithSndElemEqualToExists (fst columnAndTableAb) tableNames
  if tableExists
    then do
      let tableName = getFstTupleElemBySndElemInList columnAndTableAb tableNames
      case tableName of
        Right tn -> do
          rest <- getCorrectTableAndColumns xs tableNames
          return $ (tn, snd columnAndTableAb) : rest
        Left err -> EitherT $ return $ Left err
    else
      EitherT $ return $ Left "Error parsing column table abbreviations"

getCorrectTableAndColumns _ _ = EitherT $ return $ Left "Something went wrong when trying to find match for table abbreviation"

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
getListOfTableAbAndColumn [originalAbAndColumn] = if abAndColumn == "now()" then ["now()", "now()"] else wordsWhen (== '.') abAndColumn
  where
    abAndColumn = if Data.List.last originalAbAndColumn == ',' then init originalAbAndColumn else originalAbAndColumn
getListOfTableAbAndColumn (originalAbAndColumn : xs) = (if abAndColumn == "now()" then ["now()", "now()"] else wordsWhen (== '.') abAndColumn) Data.List.++ getListOfTableAbAndColumn xs
  where
    abAndColumn = if Data.List.last originalAbAndColumn == ',' then init originalAbAndColumn else originalAbAndColumn

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

parseAggregate :: [TableName] -> Maybe WhereClause -> [SelectColumn] -> Maybe SortClause -> ParsedStatementT ParsedStatement
parseAggregate tableNames whereClause columns clause = return $ SelectAggregate tableNames columns whereClause clause

-- ignores commas in columns
parseColumnsSelect :: [TableName] -> [(String, String)] -> Maybe WhereClause-> Maybe SortClause -> ParsedStatementT ParsedStatement
parseColumnsSelect tables columns whereClause sortClause = return $ SelectColumns tables (Data.List.map (\(tname, cname) -> if tname == "now()" && cname == "now()" then Now else TableColumn tname cname) columns) whereClause sortClause

parseAllColumns :: [TableName] -> Maybe WhereClause -> Maybe SortClause-> ParsedStatementT ParsedStatement
parseAllColumns tableNames whereClause sortClause = return $ SelectAll tableNames whereClause sortClause

getSelectType :: [String] -> ParsedStatementT SelectType
getSelectType selectColumns
  | hasAllValidAggregates selectColumns = return Aggregate
  | hasAllValidColumns selectColumns = return ColumnsAndTime
  | isValidSelectAll selectColumns = return AllColumns
  | otherwise = EitherT $ return $ Left "Error parsing select type. Maybe invalid table abbreviation was used or an invalid query was given"

isValidSelectAll :: [String] -> Bool
isValidSelectAll selectColumns
  | Data.List.length selectColumns == 1 && Data.List.head selectColumns == "*" = True
  | otherwise = False

hasAllValidColumns :: [String] -> Bool
hasAllValidColumns [] = False
hasAllValidColumns [column] = isValidColumn column
hasAllValidColumns (column : xs) = isValidColumn column && hasAllValidColumns xs

isValidColumn :: String -> Bool
isValidColumn column = isSplitByDot && isNotAggregate && hasNoParenthesies || removedComma == "now()"
  where
    removedComma = if "," `Data.List.isSuffixOf` column then init column else column
    (beforeDot, afterDot) = Data.List.break (== '.') column
    isSplitByDot = not (Data.List.null beforeDot) && not (Data.List.null afterDot) && Data.List.length afterDot > 1
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
  | ("max(" `Data.List.isPrefixOf` removedCommaColumn || "avg(" `Data.List.isPrefixOf` removedCommaColumn) && ")" `Data.List.isSuffixOf` removedCommaColumn && isValidColumn (Data.List.drop 4 (Data.List.init removedCommaColumn)) = True
  | removedCommaColumn == "now()" = True
  | otherwise = False
    where
      removedCommaColumn = if Data.List.last column == ',' then Data.List.init column else column
statementClause :: [String] -> [(TableName, String)] -> ParsedStatementT (Maybe WhereClause)
statementClause afterWhere tableNames = case Data.List.length afterWhere of
  0 -> return Nothing
  _ -> tryParseWhereClause afterWhere tableNames

tryParseWhereClause :: [String] -> [(TableName, String)] -> ParsedStatementT (Maybe WhereClause)
tryParseWhereClause afterWhere tableNames
  | isBoolIsTrueFalseClauseLike = case splitStatementToWhereIsClause (Data.List.drop 1 afterWhere) tableNames of
    Left err -> EitherT $ return $ Left err
    Right clause -> return $ Just clause
  | isAndClauseLike = case parseWhereAnd (Data.List.drop 1 afterWhere) tableNames of
    Left err -> EitherT $ return $ Left err
    Right clause -> return $ Just clause
  | otherwise = EitherT $ return $ Left "Failed to parse where clause. Where clause type not implemented or recognised. Please only use `where and` and `where bool is true/false`"
  where
    afterWhereWithoutWhere = Data.List.drop 1 afterWhere
    (_, afterIs) = Data.List.break (== "is") afterWhereWithoutWhere
    firstElementIsColumn = not (Data.List.null afterWhereWithoutWhere) && Data.List.length (wordsWhen (== '.') (Data.List.head afterWhereWithoutWhere)) == 2
    isBoolIsTrueFalseClauseLike = Data.List.length afterWhereWithoutWhere == 3 && Data.List.length afterIs == 2 && (afterIs Data.List.!! 1 == "false" || afterIs Data.List.!! 1 == "true") && firstElementIsColumn
    isAndClauseLike = Data.List.null afterIs

splitStatementToWhereIsClause :: [String] -> [(TableName, String)] -> Either ErrorMessage WhereClause
splitStatementToWhereIsClause [boolColName, "is", boolString] tableNames = do
   validTableName <- getFstTupleElemBySndElemInList (tableName, colName) tableNames
   Right $ IsValueBool parsedBoolString validTableName $ Data.List.drop 1 colName
  where
    (tableName, colName) = Data.List.break (== '.') boolColName
    parsedBoolString = boolString == "true"
splitStatementToWhereIsClause _ _ = Left "Unsupported or invalid where bool is true false clause. Maybe the formating is wrong?"

parseWhereAnd :: [String] -> [(TableName, String)] -> Either ErrorMessage WhereClause
parseWhereAnd afterWhere tableNames
  | matchesWhereAndPattern afterWhereBeforeOrder = splitStatementToAndClause afterWhereBeforeOrder
  | otherwise = Left "Unable to parse WHERE AND clause"
  where
    afterWhereBeforeOrder = takeWhile (/= "order") afterWhere
    splitStatementToAndClause :: [String] -> Either ErrorMessage WhereClause
    splitStatementToAndClause strList = do
      conditionList <- getConditionList strList
      getConditions conditionList

    getConditions :: [Condition] -> Either ErrorMessage WhereClause
    getConditions conditions = Right $ Conditions conditions

    getConditionList :: [String] -> Either ErrorMessage [Condition]
    getConditionList [condition1, operator, condition2] = do
      condition <- getCondition condition1 operator condition2 tableNames
      return [condition]

    getConditionList (condition1 : operator : condition2 : "and" : xs) = do
      conditionBase <- getCondition condition1 operator condition2 tableNames
      rest <- if not (Data.List.null xs) then getConditionList xs else Right []
      return $ conditionBase : rest

    getConditionList _ = Left "Error parsing where and clause"

getConditionValue :: String -> Either ErrorMessage ConditionValue
getConditionValue condition
  | isNumber condition = Right $ IntValue (read condition :: Integer)
  | Data.List.length condition > 2 && "'" `Data.List.isPrefixOf` condition && "'" `Data.List.isSuffixOf` condition = Right $ StrValue (Data.List.drop 1 (Data.List.init condition))
  | otherwise = Left "Error parsing condition value"

getCondition :: String -> String -> String -> [(TableName, String)] -> Either ErrorMessage Condition
getCondition val1 op val2 tableNames
  | op == "=" && isRight val1TableEither && isRight val2TableEither && val1HasDot && val2HasDot = Right $ ColumnValuesEqual (TableColumn val1Table (Data.List.drop 1 val1Column)) (TableColumn val2Table (Data.List.drop 1 val2Column))
  | op == "=" && isRight val1TableEither && val1HasDot && isJust value2Type && isNothing value1Type && isCondition2 = Right $ Equals (TableColumn val1Table (Data.List.drop 1 val1Column)) $ castEither defaultCondition condition2
  | op == "=" && isRight val2TableEither && val2HasDot && isJust value1Type && isNothing value2Type && isCondition1= Right $ Equals (TableColumn val2Table (Data.List.drop 1 val2Column)) $ castEither defaultCondition condition1
  | op == ">" && isRight val1TableEither && val1HasDot && isJust value2Type && isNothing value1Type && isCondition2 = Right $ GreaterThan (TableColumn val1Table (Data.List.drop 1 val1Column)) $ castEither defaultCondition condition2
  | op == ">" && isRight val2TableEither && val2HasDot && isJust value1Type && isNothing value2Type && isCondition1 = Right $ GreaterThan (TableColumn val2Table (Data.List.drop 1 val2Column)) $ castEither defaultCondition condition1
  | op == "<" && isRight val1TableEither && val1HasDot && isJust value2Type && isNothing value1Type && isCondition2 = Right $ LessThan (TableColumn val1Table (Data.List.drop 1 val1Column)) $ castEither defaultCondition condition2
  | op == "<" && isRight val2TableEither && val2HasDot && isJust value1Type && isNothing value2Type && isCondition1 = Right $ LessThan (TableColumn val2Table (Data.List.drop 1 val2Column)) $ castEither defaultCondition condition1
  | op == ">=" && isRight val1TableEither && val1HasDot && isJust value2Type && isNothing value1Type && isCondition2 = Right $ GreaterThanOrEqual (TableColumn val1Table (Data.List.drop 1 val1Column)) $ castEither defaultCondition condition2
  | op == ">=" && isRight val2TableEither && val2HasDot && isJust value1Type && isNothing value2Type && isCondition1 = Right $ GreaterThanOrEqual (TableColumn val2Table (Data.List.drop 1 val2Column)) $ castEither defaultCondition condition1
  | op == "<=" && isRight val1TableEither && val1HasDot && isJust value2Type && isNothing value1Type && isCondition2 = Right $ LessthanOrEqual (TableColumn val1Table (Data.List.drop 1 val1Column)) $ castEither defaultCondition condition2
  | op == "<=" && isRight val2TableEither && val2HasDot && isJust value1Type && isNothing value2Type && isCondition1 = Right $ LessthanOrEqual (TableColumn val2Table (Data.List.drop 1 val2Column)) $ castEither defaultCondition condition1
  | op == "<>" && isRight val1TableEither && val1HasDot && isJust value2Type && isNothing value1Type && isCondition2 = Right $ NotEqual (TableColumn val1Table (Data.List.drop 1 val1Column)) $ castEither defaultCondition condition2
  | op == "<>" && isRight val2TableEither && val2HasDot && isJust value1Type && isNothing value2Type && isCondition1 = Right $ NotEqual (TableColumn val2Table (Data.List.drop 1 val2Column)) $ castEither defaultCondition condition1
  | isLeft val2TableEither && isLeft val1TableEither = Left "Abbreviation in WHERE clause does not match any table in FROM part of statement"
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
    val1TableEither = getFstTupleElemBySndElemInList (val1TableAb, "I LOVE HASKELL ♥") tableNames
    val2TableEither = getFstTupleElemBySndElemInList (val2TableAb, "THIS CODE I SO GREAT. ITS THE BEST CODE. THE BEST") tableNames

    val1Table = castEither "If you are the user of this app and you are seeing this. I am sorry to inform you that the creators of this program suck at coding" val1TableEither
    val2Table = castEither "If you are the user of this app and you are seeing this. I am sorry to inform you that the creators of this program suck at coding" val2TableEither

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


statementClause' :: [String] -> TableName -> ParsedStatementT (Maybe WhereClause)
statementClause' afterWhere tableName = case Data.List.length afterWhere of
  0 -> return Nothing
  _ -> tryParseWhereClause' afterWhere tableName

tryParseWhereClause' :: [String] -> TableName -> ParsedStatementT (Maybe WhereClause)
tryParseWhereClause' afterWhere tableName
  | isBoolIsTrueFalseClauseLike = case splitStatementToWhereIsClause' afterWhere tableName of
    Left err -> EitherT $ return $ Left err
    Right clause -> return $ Just clause
  | isAndClauseLike = case parseWhereAnd' (Data.List.drop 1 afterWhere) tableName of
    Left err -> EitherT $ return $ Left err
    Right clause -> return $ Just clause
  | otherwise = EitherT $ return $ Left "Failed to parse where clause. Where clause type not implemented or recognised. Please only use `where and` and `where bool is true/false`"
  where
    afterWhereWithoutWhere = Data.List.drop 1 afterWhere
    (_, afterIs) = Data.List.break (== "is") afterWhereWithoutWhere
    firstElementIsColumn = not (Data.List.null afterWhereWithoutWhere) && Data.List.length (wordsWhen (== '.') (Data.List.head afterWhereWithoutWhere)) == 1
    isBoolIsTrueFalseClauseLike = Data.List.length afterWhereWithoutWhere == 3 && Data.List.length afterIs == 2 && (afterIs Data.List.!! 1 == "false" || afterIs Data.List.!! 1 == "true") && firstElementIsColumn
    isAndClauseLike = Data.List.null afterIs

splitStatementToWhereIsClause' :: [String] -> TableName -> Either ErrorMessage WhereClause
splitStatementToWhereIsClause' [boolColName, "is", boolString] tableName = Right $ IsValueBool parsedBoolString tableName $ Data.List.init boolColName
  where
    parsedBoolString = boolString == "true"
splitStatementToWhereIsClause' _ _ = Left "Unsupported or invalid where bool is true false clause"


parseWhereAnd' :: [String] -> TableName -> Either ErrorMessage WhereClause
parseWhereAnd' afterWhere tableName
  | matchesWhereAndPattern' afterWhereBeforeOrder = splitStatementToAndClause afterWhereBeforeOrder tableName
  | otherwise = Left "Unable to parse where and clause"
  where
    afterWhereBeforeOrder = takeWhile (/= "order") afterWhere
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
  | "avg(" `Data.List.isPrefixOf` Data.List.map Data.Char.toLower word && ")" `Data.List.isSuffixOf` word = "avg(" Data.List.++ Data.List.drop 4 (Data.List.init word) Data.List.++ ")"
  | "max(" `Data.List.isPrefixOf` Data.List.map Data.Char.toLower word && ")" `Data.List.isSuffixOf` word = "max(" Data.List.++ Data.List.drop 4 (Data.List.init word) Data.List.++ ")"
  | otherwise = word
  where
    keywords = ["select", "from", "where", "show", "table", "tables", "false", "true", "and", "is", "insert", "delete", "update", "set", "into", "values", "order", "by", "asc", "desc", "create", "drop"]

-- ===============================================================
-- End of Parser


-- ===============================================================
  --Start of folterRows


filterRows :: DataFrame -> Maybe WhereClause -> Either String DataFrame
filterRows df@(DataFrame cols _) (Just wc) =
    if whereClauseHasValidColumns wc cols
    then deleteRowsFromDataFrame df wc
    else Left "Error: Specified column in WhereClause does not exist."
filterRows (DataFrame cols _) Nothing = Right $ DataFrame cols []

whereClauseHasValidColumns :: WhereClause -> [Column] -> Bool
whereClauseHasValidColumns (IsValueBool _ _ columnName) cols = isJust (findColumnIndex columnName cols)
whereClauseHasValidColumns (Conditions conditions) cols = Data.List.all (`conditionHasValidColumn` cols) conditions

conditionHasValidColumn :: Condition -> [Column] -> Bool
conditionHasValidColumn condition cols = isJust (findColumnIndex (columnNameFromCondition condition) cols)

columnNameFromCondition :: Condition -> String
columnNameFromCondition (Equals colName _) = extractColumnName colName
columnNameFromCondition (GreaterThan colName _) = extractColumnName colName
columnNameFromCondition (LessThan colName _) = extractColumnName colName
columnNameFromCondition (LessthanOrEqual colName _) = extractColumnName colName
columnNameFromCondition (GreaterThanOrEqual colName _) = extractColumnName colName
columnNameFromCondition (NotEqual colName _) = extractColumnName colName
columnNameFromCondition _ = "Error getting column name"

extractColumnName :: SelectColumn -> String
extractColumnName (TableColumn _ colName) = colName
extractColumnName _ = error "Unsupported SelectColumn type for condition"

deleteRowsFromDataFrame :: DataFrame -> WhereClause -> Either String DataFrame
deleteRowsFromDataFrame (DataFrame cols rows) wc =
    let nonMatchingRows = filter (not . rowSatisfiesWhereClause wc cols) rows
    in if length nonMatchingRows == length rows
       then Left "Error: No rows deleted. None match the specified condition."
       else Right $ DataFrame cols nonMatchingRows

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
    compareWithCondition (columnNameFromSelectColumn colName) cols row (==) condValue
conditionSatisfied (GreaterThan colName condValue) (cols, row) =
    compareWithCondition (columnNameFromSelectColumn colName) cols row (>) condValue
conditionSatisfied (LessThan colName condValue) (cols, row) =
    compareWithCondition (columnNameFromSelectColumn colName) cols row (<) condValue
conditionSatisfied (LessthanOrEqual colName condValue) (cols, row) =
    compareWithCondition (columnNameFromSelectColumn colName) cols row (<=) condValue
conditionSatisfied (GreaterThanOrEqual colName condValue) (cols, row) =
    compareWithCondition (columnNameFromSelectColumn colName) cols row (>=) condValue
conditionSatisfied (NotEqual colName condValue) (cols, row) =
    compareWithCondition (columnNameFromSelectColumn colName) cols row (/=) condValue
conditionSatisfied (ColumnValuesEqual col1 col2) (cols, row) = row !! realCol1Value == row !! realCol2Value
  where
    realCol1Value = fromMaybe (-1) $ getColumnIndexByName (columnNameFromSelectColumn col1) cols
    realCol2Value = fromMaybe (-1) $ getColumnIndexByName (columnNameFromSelectColumn col2) cols

-- This fails when columns can have the same names in different tables
getColumnIndexByName :: String -> [Column] -> Maybe Int
getColumnIndexByName cName cols = elemIndex cName $ map (\(Column colName _) -> colName) cols

columnNameFromSelectColumn :: SelectColumn -> String
columnNameFromSelectColumn (TableColumn _ colName) = colName
columnNameFromSelectColumn _ = error "Unsupported SelectColumn type for condition"

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
findColumnIndex columnName = Data.List.findIndex (\(Column name _) -> name == columnName)

-- Start of insert 
-- =============================================

createRowFromValues :: [ColumnName] -> [Column] -> [Value] -> Either ErrorMessage Row

createRowFromValues insertCols [col] values = if not (null insertCols || null values) && getColumnName col `elem` insertCols
  then do
    let colIndex = fromMaybe (-1) $ Data.List.elemIndex (getColumnName col) insertCols
    element <- Right $ values !! colIndex
    return [element]
  else
    return [NullValue]

createRowFromValues insertCols (col : xs) values = if not (null insertCols || null values) && getColumnName col `elem` insertCols
  then do
    let colIndex = fromMaybe (-1) (Data.List.elemIndex (getColumnName col) insertCols)
    element <- Right $ values !! colIndex
    rest <- createRowFromValues (removeAtIndex colIndex insertCols) xs (removeAtIndex colIndex values)
    return $ element : rest
  else do
    rest <- createRowFromValues insertCols xs values
    return $ NullValue : rest


createRowFromValues _ _ _ = Left "Should not be here"

getColumnName :: Column -> String
getColumnName (Column name _) = name

removeAtIndex :: Int -> [a] -> [a]
removeAtIndex index list
    | index < 0 || index >= length list = list
    | otherwise = take index list ++ drop (index + 1) list


-- End of Insert 
-- ========================================================================
--Start of Update
updateRowsInTable :: TableName -> SelectedColumns -> Row -> Maybe WhereClause -> DataFrame -> Either ErrorMessage DataFrame
updateRowsInTable _ columns newRow maybeWhereClause (DataFrame dfColumns dfRows) =
    Right $ DataFrame dfColumns (map updateRowIfRequired dfRows)
  where
    updateRowIfRequired row =
        case maybeWhereClause of
            Just whereClause ->
                if rowSatisfiesWhereClause whereClause dfColumns row
                then updateRowValues dfColumns columns newRow row
                else row
            Nothing -> updateRowValues dfColumns columns newRow row

updateRowValues :: [Column] -> SelectedColumns -> Row -> Row -> Row
updateRowValues columns selectCols newRow = zipWith (curry updateValue) [0..]
  where
    colNames = map extractColumnName selectCols
    colIndices = mapMaybe (`findColumnIndex` columns) colNames
    newValueMap = zip colIndices newRow

    updateValue (idx, value) =
        fromMaybe value (lookup idx newValueMap)

-- ========================================================================
-- End of Update

extractColumnNames :: SelectedColumns -> [ColumnName]
extractColumnNames = mapMaybe extractName
  where
    extractName :: SelectColumn -> Maybe ColumnName
    extractName (TableColumn _ colName) = Just colName
    extractName _ = Nothing

instance Ord Value where
compare (IntegerValue x) (IntegerValue y) = Prelude.compare x y
compare (StringValue x) (StringValue y) = Prelude.compare x y
compare (BoolValue x) (BoolValue y) = Prelude.compare x y
compare _ _ = EQ

extractSelectedColumnsRows :: [Lib4.TableName] -> [Lib4.SelectColumn] -> [(Lib4.TableName, DataFrame)] -> Maybe WhereClause -> [Row]
extractSelectedColumnsRows selectedTables selectedColumns tables whereClause = filteredRows
    -- concatMap extractRowsFromTable filteredTables
    where
        filteredTables = filter (\(name, _) -> name `elem` selectedTables) tables
        filterdTablesJointDf = case getFilteredDataFrame (map snd filteredTables) selectedColumns of
          Right df -> df
          Left _ -> DataFrame [] []

        rawJointDataFrame = case extractMultipleTableDataframe (map snd filteredTables) of
          Right rawDf -> rawDf
          Left _ -> DataFrame [] []

        dataFrameRows = getDataFrameRows filterdTablesJointDf
        rawRowsWithIndex = zip [0, 1..] $ getDataFrameRows rawJointDataFrame

        filteredRows = case whereClause of
          Just wc -> mapMaybe (\(idx, row) -> if rowSatisfiesWhereClause wc (getDataFrameColumns rawJointDataFrame) row then Just (dataFrameRows !! idx) else Nothing) rawRowsWithIndex
          Nothing -> dataFrameRows

getFilteredDataFrame :: [DataFrame] -> [SelectColumn] -> Either ErrorMessage DataFrame
getFilteredDataFrame dfs selectedCols = do
  baseDf <- extractMultipleTableDataframe dfs
  selectedColumnNames <- Right $ map columnNameFromSelectColumn selectedCols
  columnNamesWithIndex <- Right $ zip [0, 1..] $ map getColumnName $ getDataFrameColumns baseDf
  realColumns <- Right $ filter (\col -> getColumnName col `elem` selectedColumnNames) $ getDataFrameColumns baseDf
  realColumnNamesWithIndex <- Right $ filter (\(_, name) -> name `elem` selectedColumnNames) columnNamesWithIndex
  realColumnIndexes <- Right $ map fst realColumnNamesWithIndex
  filteredRows <- filterRowsByIndex (getDataFrameRows baseDf) realColumnIndexes
  return (DataFrame realColumns filteredRows)

filterRowsByIndex :: [Row] -> [Integer] -> Either ErrorMessage [Row]
filterRowsByIndex [] _ = Right []
filterRowsByIndex [baseRow] idxs = do
  fRow <- filterRow baseRow idxs
  return [fRow]
filterRowsByIndex (baseRow : xs) idxs = do
  fRow <- filterRow baseRow idxs
  rest <- filterRowsByIndex xs idxs
  return $ fRow : rest

filterRow :: Row -> [Integer] -> Either ErrorMessage Row
filterRow row idxs = Right $ map snd $ filter (\(idx, _) -> idx `elem` idxs) (Data.List.zip [0, 1..] row)

extractMultipleTableDataframe :: [DataFrame] -> Either ErrorMessage DataFrame
extractMultipleTableDataframe [] = Left "No no tables provided"
extractMultipleTableDataframe [df] = Right df
extractMultipleTableDataframe ((DataFrame cols rows) : xs) = do
  rest <- extractMultipleTableDataframe xs
  return $ DataFrame (cols ++ getDataFrameColumns rest) (multiplyRows (getDataFrameRows rest) rows)

multiplyRows :: [Row] -> [Row] -> [Row]
multiplyRows [row] rowsToMultiply = map (++ row) rowsToMultiply
multiplyRows (baseRow : xs) rowsToMultiply = map (++ baseRow) rowsToMultiply ++ multiplyRows xs rowsToMultiply
multiplyRows _ _ = []


getDataFrameColumns :: DataFrame -> [Column]
getDataFrameColumns (DataFrame cols _) = cols

getDataFrameRows :: DataFrame -> [Row]
getDataFrameRows (DataFrame _ rows) = rows

extractAggregateRows :: [Lib4.TableName] -> [Lib4.SelectColumn] -> Maybe Lib4.WhereClause -> [(Lib4.TableName, DataFrame)] -> [Row]
extractAggregateRows tableNames aggFuncs whereClause tables =
    let filteredTables = filter (\(name, _) -> name `elem` tableNames) tables
        aggregatedRows = map (applyAggregateFunction filteredTables whereClause) aggFuncs
    in [concat aggregatedRows]

applyAggregateFunction :: [(Lib4.TableName, DataFrame)] -> Maybe Lib4.WhereClause -> Lib4.SelectColumn -> [Value]
applyAggregateFunction tables whereClause aggFunc =
    case aggFunc of
        Lib4.Max tableName colName -> [maxAggregate tableName colName tables whereClause]
        Lib4.Avg tableName colName -> [avgAggregate tableName colName tables whereClause]
        _ -> error "Unsupported aggregate function"

maxAggregate :: Lib4.TableName -> String -> [(Lib4.TableName, DataFrame)] -> Maybe Lib4.WhereClause -> Value
maxAggregate _ colName tables whereClause =
    let tableDf = case extractMultipleTableDataframe (map snd tables) of
          Right df -> df
          Left _ -> DataFrame [] []
        filteredRows = case whereClause of
          Just wc -> mapMaybe (\row -> if rowSatisfiesWhereClause wc (getDataFrameColumns tableDf) row then Just row else Nothing) (getDataFrameRows tableDf)
          Nothing -> getDataFrameRows tableDf

        rowIndex = fromMaybe (-1) $ getColumnIndexByName colName (getDataFrameColumns tableDf)
        filteredValues = map (!! rowIndex) filteredRows
        maxVal = maximum $ mapMaybe unwrapInteger filteredValues
    in IntegerValue maxVal


avgAggregate :: Lib4.TableName -> String -> [(Lib4.TableName, DataFrame)] -> Maybe Lib4.WhereClause -> Value
avgAggregate _ colName tables whereClause =
    let tableDf = case extractMultipleTableDataframe (map snd tables) of
          Right df -> df
          Left _ -> DataFrame [] []
        filteredRows = case whereClause of
          Just wc -> mapMaybe (\row -> if rowSatisfiesWhereClause wc (getDataFrameColumns tableDf) row then Just row else Nothing) (getDataFrameRows tableDf)
          Nothing -> getDataFrameRows tableDf

        rowIndex = fromMaybe (-1) $ getColumnIndexByName colName (getDataFrameColumns tableDf)
        filteredValues = map (!! rowIndex) filteredRows
        values = mapMaybe unwrapInteger filteredValues
        avgVal = sum values `div` fromIntegral (length values)
    in IntegerValue avgVal

unwrapInteger :: Value -> Maybe Integer
unwrapInteger (IntegerValue i) = Just i
unwrapInteger _ = Nothing
