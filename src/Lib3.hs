{-# LANGUAGE DeriveFunctor #-}

module Lib3
  ( executeSql,
    Execution,
    ExecutionAlgebra(..),
    loadFiles,
    getTime,
  )
where

import Control.Monad.Free (Free (..), liftF)
import DataFrame (Column(..), DataFrame(..), Row, ColumnType (IntegerType, StringType))
import Data.Time (UTCTime)
import Data.List (isPrefixOf, isSuffixOf)
import Data.Char (isDigit)
import Data.Maybe (isNothing, isJust)
type TableName = String
type FileContent = String
type ErrorMessage = String
type SQLQuery = String
type ColumnName = String

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
  | GetTableDfByName TableName [(TableName, DataFrame)] (DataFrame -> next)
  | ParseTables [FileContent] ([(TableName, DataFrame)] -> next)
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

getTableDfByName :: TableName -> [(TableName, DataFrame)] -> Execution DataFrame
getTableDfByName tableName tables = liftF $ GetTableDfByName tableName tables id

parseTables :: [FileContent] -> Execution [(TableName, DataFrame)]
parseTables content = liftF $ ParseTables content id

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

  tableNames    <- getTableNames parsedStatement
  tableFiles    <- loadFiles tableNames
  tables        <- parseTables tableFiles
  isValid       <- isParsedStatementValid parsedStatement tables
  statementType <- getStatementType statement
  timeStamp     <- getTime

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