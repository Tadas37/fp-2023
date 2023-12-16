module Parser
  ( 
    parseStatement
  )
where

import DataFrame (Column(..), ColumnType(..), Value(..), Row, DataFrame(..))
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
      isInfixOf )
import Data.Char (toLower, isDigit)
import Data.Either (isRight, isLeft, partitionEithers)
import Prelude hiding (zip)
import Data.Maybe ( mapMaybe, isJust, isNothing, fromMaybe )

type TableName = String
type FileContent = String
type ErrorMessage = String
type SQLQuery = String
type ColumnName = String

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
  = ColumnSort [ColumnName] (Maybe SortOrder)

data SortOrder
  = Asc
  | Desc

data SelectType = Aggregate | ColumnsAndTime | AllColumns

data StatementType = Select | Delete | Insert | Update | ShowTable | ShowTables | InvalidStatement
