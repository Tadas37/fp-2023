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


-- parseStatement :: String -> Either ErrorMessage ParsedStatement
-- parseStatement input
--   | last input /= ';' = Left "Unsupported or invalid statement"
--   | otherwise = mapStatementType wordsInput
--   where
--     cleanedInput = init input
--     wordsInput = parseSemiCaseSensitive cleanedInput

-- mapStatementType :: [String] -> Either ErrorMessage ParsedStatement
-- mapStatementType statement = case statement of
--   ["show", "table", tableName] ->
--     if tableNameExists tableName
--       then Right (ShowTable tableName)
--       else Left "Table not found"
--   ["show", "tables"] -> Right ShowTables
--   "select" : rest -> parseSelect rest
--   _ -> Left "Unsupported or invalid statement"

mapStatementType :: [String] -> Either ErrorMessage ParsedStatement
mapStatementType statement = do
  statementType <- guessStatementType statement
  case statementType of
      Select -> do
        parseSelect statement
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
  | x == "insert" = Right Insert
  | x == "delete" = Right Delete
  | x == "update" = Right Update
  | x == "show" && y == "table" = Right ShowTable
  | x == "show" && y == "tables" = Right ShowTables
  | otherwise = Left "Sql statement does not resemble any sort of implemented statement"

guessStatementType _ = Left "Sql statement does not resemble any sort of implemented statement"

castEither :: a -> Either ErrorMessage a -> a
castEither defaultValue eitherValue = case eitherValue of
  Right val -> val
  Left err -> defaultValue

-- parseSelect :: [String] -> Either ErrorMessage ParsedStatement
-- parseSelect statement = parseFunctionBody
--   where
--     (_, afterWhere) = break (== "where") statement
--     (_, afterIs) = break (== "is") afterWhere
--     (columnWords, fromAndWhere) = break (== "from") statement

--     wordsAfterWhere = length afterWhere
--     hasWhereClause = wordsAfterWhere > 0
--     isBoolIsTrueFalseClauseLike = hasWhereClause && length afterIs == 2
--     isAndClauseLike = hasWhereClause && null afterIs

--     statementClause :: Either ErrorMessage (Maybe WhereClause)
--     statementClause
--       | not hasWhereClause && length fromAndWhere == 2 = Right Nothing
--       | isBoolIsTrueFalseClauseLike = case parseWhereBoolIsTrueFalse fromAndWhere of
--         Left err -> Left err
--         Right clause -> Right $ Just clause
--       | isAndClauseLike = case parseWhereAnd fromAndWhere of
--         Left err -> Left err
--         Right clause -> Right $ Just clause
--       | otherwise = Left "Unsupported or invalid statement"

--     columnName = drop 4 $ init (head columnWords)

--     tableName
--       | length fromAndWhere >= 2 && head fromAndWhere == "from" = fromAndWhere !! 1
--       | otherwise = ""

--     columnString = unwords columnWords
--     columnNames = map (dropWhile (== ' ')) $ splitByComma columnString

--     parseFunctionBody :: Either ErrorMessage ParsedStatement
--     parseFunctionBody = case statementClause of
--       Left err -> Left err
--       Right clause
--         | not (null columnNames) && listConsistsOfValidAggregates columnNames tableName -> Right $ AggregateSelect tableName (map parseAggregate columnNames) clause
--         | length columnWords == 1 && head columnWords == "*"-> Right (SelectAll tableName clause)
--         | not (null columnNames) && all (columnNameExists tableName) columnNames -> Right (SelectColumns tableName columnNames clause)
--         | otherwise -> Left "Unsupported or invalid statement"

getTableNamesAndAb :: [String] -> Either ErrorMessage [(TableName, String)]
getTableNamesAndAb statement = names
  where
    (_, afterFrom) = break (== "from") statement
    (tables, _) = break (== "where") afterFrom
    len = length tables

    names
      | even len && len > 0 = Right $ splitIntoTuples tables
      | otherwise = Left "Invalid table formating in statement"



getColumnWithTableAb :: [String] -> Either ErrorMessage [(String, String)]
getColumnWithTableAb statement = do
  (beforeFrom, _) <- Right $ break (== "from") statement
  if null beforeFrom || odd (length beforeFrom)
    then do
      Left "error parsing columns"
    else do
    Right $ splitIntoTuples beforeFrom


splitIntoTuples :: [String] -> [(String, String)]
splitIntoTuples (x : y : xs) = (x, y) : splitIntoTuples xs

parseSelect :: [String] -> Either ErrorMessage ParsedStatement
parseSelect statement = do
  tableNamesAndAb <- getTableNamesAndAb statement
  columnsWithTableAb <- getColumnWithTableAb statement
  (beforeWhere, afterWhere) <- Right $ break (== "where") statement
  whereClause <- statementClause afterWhere

  
  Left "error"

statementClause :: [String] -> Either ErrorMessage (Maybe WhereClause)
statementClause afterWhere = do
  case length afterWhere of
    0 -> do
      Right Nothing
    _ -> do
      tryParseWhereClause afterWhere

tryParseWhereClause :: [String] -> Either ErrorMessage (Maybe WhereClause)
tryParseWhereClause afterWhere
  | isBoolIsTrueFalseClauseLike = case parseWhereBoolIsTrueFalse afterWhere of
    Left err -> Left err
    Right clause -> Right $ Just clause
  | isAndClauseLike = case parseWhereAnd afterWhere of
    Left err -> Left err
    Right clause -> Right $ Just clause
  | otherwise = Left "Unsupported or invalid statement"
  where
    afterIs = break (== "is") afterWhere
    isBoolIsTrueFalseClauseLike = length afterIs == 2
    isAndClauseLike = null afterIs

parseWhereBoolIsTrueFalse :: [String] ->Either ErrorMessage WhereClause
parseWhereBoolIsTrueFalse fromWhere
  | matchesWhereBoolTrueFalsePatern = splitStatementToWhereIsClause fromWhere
  | otherwise = Left "Unsupported or invalid statement"
  where
    matchesWhereBoolTrueFalsePatern = length fromWhere == 4 && head fromWhere == "where" && fromWhere !! 2 == "is" && (fromWhere !! 3 == "false" || fromWhere !! 3 == "true" )

splitStatementToWhereIsClause :: [String] -> Either ErrorMessage WhereClause
splitStatementToWhereIsClause ["where", boolColName, "is", boolString] = Right $ IsValueBool parsedBoolString tableName $ init colName
  where
    (tableName, colName) = break (== '.') boolColName
    parsedBoolString = boolString == "true"
splitStatementToWhereIsClause _ = Left "Unsupported or invalid where bool is true false clase"


parseWhereAnd :: [String] -> Either ErrorMessage WhereClause
parseWhereAnd afterWhere
  | matchesWhereAndPattern (drop 1 afterWhere) = splitStatementToAndClause (drop 1 afterWhere)
  | otherwise = Left "Unsupported or invalid statement"
  where
    splitStatementToAndClause strList = do
      conditionList <- getConditionList strList
      getConditions conditionList

    getConditions :: [Condition] -> Either ErrorMessage WhereClause
    getConditions conditions = Right $ Conditions conditions

    getConditionList :: [String] -> Either ErrorMessage [Condition]
    getConditionList [condition1, operator, condition2] = do
      condition <- getCondition condition1 operator condition2
      return [condition]

    getConditionList (condition1 : operator : condition2 : _ : xs) = do
      conditionBase <- getCondition condition1 operator condition2
      rest <- getConditionList xs
      return $ conditionBase : rest

    getConditionList _ = Left "Error parsing where and clause"

getConditionValue :: String -> Either ErrorMessage ConditionValue
getConditionValue condition
  | isNumber condition = Right $ IntValue (read condition :: Integer)
  | length condition > 2 && "'" `isPrefixOf` condition && "'" `isSuffixOf` condition = Right $ StrValue (drop 1 (init condition))
  | otherwise = Left "Error parsing condition value"

getCondition :: String -> String -> String -> Either ErrorMessage Condition
getCondition val1 op val2
  | op == "=" && val1HasDot && isJust value2Type && isNothing value1Type && isCondition2 = Right $ Equals (TableColumn val1TableAb val1Column) $ castEither defaultCondition condition2
  | op == "=" && val2HasDot && isJust value1Type && isNothing value2Type && isCondition1= Right $ Equals (TableColumn val2TableAb val2Column) $ castEither defaultCondition condition1
  | op == ">" && val1HasDot && isJust value2Type && isNothing value1Type && isCondition2 = Right $ GreaterThan (TableColumn val1TableAb val1Column) $ castEither defaultCondition condition2
  | op == ">" && val2HasDot && isJust value1Type && isNothing value2Type && isCondition1 = Right $ GreaterThan (TableColumn val2TableAb val2Column) $ castEither defaultCondition condition1
  | op == "<" && val1HasDot && isJust value2Type && isNothing value1Type && isCondition2 = Right $ LessThan (TableColumn val1TableAb val1Column) $ castEither defaultCondition condition2
  | op == "<" && val2HasDot && isJust value1Type && isNothing value2Type && isCondition1 = Right $ LessThan (TableColumn val2TableAb val2Column) $ castEither defaultCondition condition1
  | op == ">=" && val1HasDot && isJust value2Type && isNothing value1Type && isCondition2 = Right $ GreaterThanOrEqual (TableColumn val1TableAb val1Column) $ castEither defaultCondition condition2
  | op == ">=" && val2HasDot && isJust value1Type && isNothing value2Type && isCondition1 = Right $ GreaterThanOrEqual (TableColumn val2TableAb val2Column) $ castEither defaultCondition condition1
  | op == "<=" && val1HasDot && isJust value2Type && isNothing value1Type && isCondition2 = Right $ LessthanOrEqual (TableColumn val1TableAb val1Column) $ castEither defaultCondition condition2
  | op == "<=" && val2HasDot && isJust value1Type && isNothing value2Type && isCondition1 = Right $ LessthanOrEqual (TableColumn val2TableAb val2Column) $ castEither defaultCondition condition1
  | op == "<>" && val1HasDot && isJust value2Type && isNothing value1Type && isCondition2 = Right $ NotEqual (TableColumn val1TableAb val1Column) $ castEither defaultCondition condition2
  | op == "<>" && val2HasDot && isJust value1Type && isNothing value2Type && isCondition1 = Right $ NotEqual (TableColumn val2TableAb val2Column) $ castEither defaultCondition condition1
  | otherwise = Left "Error parsing where and condition"
  where
    (val1TableAb, val1Column) = break (=='.') val1
    (val2TableAb, val2Column) = break (=='.') val2
    value1Type = parseType val1
    value2Type = parseType val2
    val1HasDot = not (null val1Column)
    val2HasDot = not (null val1Column)
    condition1 = getConditionValue val1
    condition2 = getConditionValue val2
    defaultCondition = StrValue "Kas skaitys tas gaidys"

    isCondition1 = case condition1 of
      Right _ -> True
      Left _ -> False

    isCondition2 = case condition2 of
      Right _ -> True
      Left _ -> False


matchesWhereAndPattern :: [String] -> Bool
matchesWhereAndPattern [condition1, operator, condition2] = isWhereAndOperation [condition1, operator, condition2]
matchesWhereAndPattern (condition1 : operator : condition2 : andString : xs) = matchesWhereAndPattern [condition1, operator, condition2] && andString == "and" && matchesWhereAndPattern xs
matchesWhereAndPattern _ = False

isWhereAndOperation :: [String] -> Bool
isWhereAndOperation [condition1, operator, condition2]
  | elem operator [">", "<", "=", "<>", "<=", ">="] && col1Valid = True
  | elem operator [">", "<", "=", "<>", "<=", ">="] && col2Valid = True
  | otherwise = False
  where
    (val1Table, val1Column) = break (== '.') condition1
    value1Column = TableColumn val1Table val1Column

    (val2Table, val2Column) = break (== '.') condition2
    value2Column = TableColumn val2Table val2Column
    col1Valid = not (null (drop 1 val1Column)) && not (null val1Table)
    col2Valid = not (null (drop 1 val2Column)) && not (null val2Table)
isWhereAndOperation _ = False

parseType :: String -> Maybe ColumnType
parseType str
  | isNumber str = Just IntegerType
  | "'" `isPrefixOf` str && "'" `isSuffixOf` str = Just StringType
  | otherwise = Nothing

isNumber :: String -> Bool
isNumber "" = False
isNumber xs =
  case dropWhile isDigit xs of
    "" -> True
    _ -> False

parseInsert :: [String] -> Either ErrorMessage ParsedStatement
parseInsert statement = Left "error"

parseDelete :: [String] -> Either ErrorMessage ParsedStatement
parseDelete statement = Left "error"

parseUpdate :: [String] -> Either ErrorMessage ParsedStatement
parseUpdate statement = Left "error"

parseShowTables :: [String] -> Either ErrorMessage ParsedStatement
parseShowTables statement = Left "error"

parseShowTable :: [String] -> Either ErrorMessage ParsedStatement
parseShowTable statement = Left "error"

-- listConsistsOfValidAggregates :: [String] -> TableName -> Bool
-- listConsistsOfValidAggregates [] _ = True
-- listConsistsOfValidAggregates [columnName] tableName
--   | "avg(" `isPrefixOf` columnName && ")" `isSuffixOf` columnName && isColumn && columnIsInt = True
--   | "max(" `isPrefixOf` columnName && ")" `isSuffixOf` columnName && isColumn = True
--   | otherwise = False
--   where
--     selectedColumn = init (drop 4 columnName)
--     isColumn = columnNameExists tableName selectedColumn
--     columnIsInt = getColumnType (getColumnByName selectedColumn (columns (getDataFrameByName tableName))) == IntegerType
-- listConsistsOfValidAggregates (x : xs) tableName = listConsistsOfValidAggregates [x] tableName && listConsistsOfValidAggregates xs tableName

-- parseAggregate :: String -> AggregateColumn
-- parseAggregate columnName
--   | "avg" `isPrefixOf` columnName = AvgColumn $ drop 4 $ init columnName
--   | "max" `isPrefixOf` columnName = MaxColumn $ drop 4 $ init columnName

-- parseWhereBoolIsTrueFalse :: [String] ->Either ErrorMessage WhereClause
-- parseWhereBoolIsTrueFalse fromAndWhere
--   | matchesWhereBoolTrueFalsePatern && isValidWhereClause = splitStatementToWhereClause fromAndWhere
--   | otherwise = Left "Unsupported or invalid statement"
--   where
--     matchesWhereBoolTrueFalsePatern = length fromAndWhere == 6 && head fromAndWhere == "from" && fromAndWhere !! 2 == "where" && fromAndWhere !! 4 == "is" && (fromAndWhere !! 5 == "false" || fromAndWhere !! 5 == "true" )
--     tableName = fromAndWhere !! 1
--     columnName = fromAndWhere !! 3
--     tableColumns = columns (getDataFrameByName tableName)
--     columnIsBool = getColumnType (getColumnByName columnName tableColumns) == BoolType
--     isValidWhereClause = columnNameExists tableName columnName && columnIsBool



-- remake
-- splitStatementToWhereClause :: [String] -> Either ErrorMessage WhereClause
-- splitStatementToWhereClause ["from", tableName, "where", boolColName, "is", boolString] = Right $ IsValueBool parsedBoolString tableName boolColName
--   where
--     parsedBoolString = boolString == "true"
-- splitStatementToWhereClause _ = Left "Unsupported or invalid statement"

-- -- remake
-- selectAllFromTable :: TableName -> Maybe WhereClause -> Either ErrorMessage DataFrame
-- selectAllFromTable tableName whereCondition =
--     case lookup tableName database of
--         Just _ -> Right (executeWhere whereCondition tableName)
--         Nothing -> Left $ "Table " ++ tableName ++ " not found"

-- splitByComma :: String -> [String]
-- splitByComma = map (dropWhile (== ' ')) . words . map (\c -> if c == ',' then ' ' else c)

-- parseSemiCaseSensitive :: String -> [String]
-- parseSemiCaseSensitive statement = convertedStatement
--   where
--     splitStatement = words statement
--     convertedStatement = map wordToLowerSensitive splitStatement

-- wordToLowerSensitive :: String -> String
-- wordToLowerSensitive word
--   | map toLower word `elem` keywords = map toLower word
--   | "avg(" `isPrefixOf` map toLower word && ")" `isSuffixOf` word = "avg(" ++ drop 4 (init word) ++ ")"
--   | "max(" `isPrefixOf` map toLower word && ")" `isSuffixOf` word = "max(" ++ drop 4 (init word) ++ ")"
--   | otherwise = word
--   where
--     keywords = ["select", "from", "where", "show", "table", "tables", "false", "true", "and", "is", "insert", "delete", "update", "set", "into"]

-- -- remake
-- tableNameExists :: TableName -> Bool
-- tableNameExists name = any (\(tableName, _) -> tableName == name) database

-- -- after done with parse
-- -- remake
-- executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
-- executeStatement (ShowTable tableName) =
--   case lookup tableName database of
--     Just df -> Right $ DataFrame [Column "Columns" StringType] (map (\(Column name _) -> [StringValue name]) (columns df))
--     Nothing -> Left $ "Table " ++ tableName ++ " not found"
-- executeStatement ShowTables =
--   Right $ DataFrame [Column "Tables" StringType] (map (\(name, _) -> [StringValue name]) database)
-- executeStatement (SelectColumns tableName columnNames whereCondition) = selectSpecifiedColumnsFromTable tableName columnNames whereCondition
-- executeStatement (AggregateSelect tableName aggregates whereCondition) = Right $ DataFrame parsedColumns [parsedRow]
--   where
--     df = executeWhere whereCondition tableName
--     listAggregateColumns = map (`aggregateColumnToValue` df) aggregates
--     parsedColumns = map fst listAggregateColumns
--     parsedRow = map snd listAggregateColumns

-- executeStatement (SelectAll tableName whereCondition) =
--   Right $ executeWhere whereCondition tableName

-- -- remake
-- executeWhere :: Maybe WhereClause -> TableName -> DataFrame
-- executeWhere whereClause tableName =
--     case whereClause of
--         Just (IsValueBool bool table column) ->
--             case filterRowsByBoolColumn table column bool of
--                 Right df -> df
--                 Left _   -> getDataFrameByName tableName

--         Just (Conditions conditions) ->
--             case filterRowsByConditions tableName conditions of
--                 Right df -> df
--                 Left _   -> getDataFrameByName tableName

--         Nothing ->
--             getDataFrameByName tableName

-- -- Filter rows based on whether the specified column's value is TRUE or FALSE.
-- -- remake
-- filterRowsByBoolColumn :: TableName -> String -> Bool -> Either ErrorMessage DataFrame
-- filterRowsByBoolColumn name col bool
--   | not $ isTableInDatabase name                      = Left combinedError
--   | not $ col `elem` columnNames                      = Left combinedError
--   | getColumnType currentColumn /= BoolType           = Left combinedError
--   | otherwise                                         = Right $ getRowsByBool bool currentRows
--   where
--     currentDataFrame = getDataFrameByName name
--     currentColumns   = columns currentDataFrame
--     columnNames      = getColNameList currentColumns
--     currentRows      = getDataFrameRows currentDataFrame
--     currentColumn    = getColumnByName col currentColumns

--     combinedError = "Dataframe does not exist or does not contain column by specified name or column is not of type bool"

--     getRowsByBool :: Bool -> [Row] -> DataFrame
--     getRowsByBool boolValue rows = DataFrame currentColumns $ filter (matchesBool boolValue) rows

--     matchesBool :: Bool -> Row -> Bool
--     matchesBool boolVal row = case columnIndex of
--       Just ind -> row !! ind == BoolValue boolVal
--       Nothing  -> False

--     columnIndex :: Maybe Int
--     columnIndex = elemIndex col columnNames

-- -- remake
-- filterRowsByConditions :: TableName -> [Condition] -> Either ErrorMessage DataFrame
-- filterRowsByConditions name conditions
--   | not $ isTableInDatabase name = Left "Table does not exist."
--   | otherwise = Right $ DataFrame currentColumns $ filter (matchesConditions conditions) currentRows
--   where
--     currentDataFrame = getDataFrameByName name
--     currentColumns   = columns currentDataFrame
--     currentRows      = getDataFrameRows currentDataFrame

--     matchesConditions :: [Condition] -> Row -> Bool
--     matchesConditions [] _ = True
--     matchesConditions (c:cs) row = evaluateCondition c row && matchesConditions cs row

--     evaluateCondition :: Condition -> Row -> Bool
--     evaluateCondition (Equals colName (StrValue val)) row =
--         getValueByColumnName colName row (getColNameList currentColumns) == StringValue val

--     evaluateCondition (Equals colName (IntValue val)) row =
--         case getValueByColumnName colName row (getColNameList currentColumns) of
--             IntegerValue intVal -> intVal == val
--             _ -> False

--     evaluateCondition (GreaterThan colName (StrValue val)) row =
--         case getValueByColumnName colName row (getColNameList currentColumns) of
--             StringValue strVal -> strVal > val
--             _ -> False

--     evaluateCondition (GreaterThan colName (IntValue val)) row =
--         case getValueByColumnName colName row (getColNameList currentColumns) of
--             IntegerValue intVal -> intVal > val
--             _ -> False

--     evaluateCondition (LessThan colName (StrValue val)) row =
--         case getValueByColumnName colName row (getColNameList currentColumns) of
--             StringValue strVal -> strVal < val
--             _ -> False

--     evaluateCondition (LessThan colName (IntValue val)) row =
--         case getValueByColumnName colName row (getColNameList currentColumns) of
--             IntegerValue intVal -> intVal < val
--             _ -> False

--     evaluateCondition (GreaterThanOrEqual colName (StrValue val)) row =
--         case getValueByColumnName colName row (getColNameList currentColumns) of
--             StringValue strVal -> strVal >= val
--             _ -> False

--     evaluateCondition (GreaterThanOrEqual colName (IntValue val)) row =
--         case getValueByColumnName colName row (getColNameList currentColumns) of
--             IntegerValue intVal -> intVal >= val
--             _ -> False

--     evaluateCondition (LessthanOrEqual colName (StrValue val)) row =
--         case getValueByColumnName colName row (getColNameList currentColumns) of
--             StringValue strVal -> strVal <= val
--             _ -> False

--     evaluateCondition (LessthanOrEqual colName (IntValue val)) row =
--         case getValueByColumnName colName row (getColNameList currentColumns) of
--             IntegerValue intVal -> intVal <= val
--             _ -> False

--     evaluateCondition (NotEqual colName (StrValue val)) row =
--         getValueByColumnName colName row (getColNameList currentColumns) /= StringValue val

--     evaluateCondition (NotEqual colName (IntValue val)) row =
--         case getValueByColumnName colName row (getColNameList currentColumns) of
--             IntegerValue intVal -> intVal /= val
--             _ -> False

--     getValueByColumnName :: String -> Row -> [String] -> Value
--     getValueByColumnName colName row columnNames =
--       case elemIndex colName columnNames of
--         Just ind -> row !! ind
--         Nothing  -> NullValue

-- -- remake
-- selectColumnsFromDataFrame :: Maybe WhereClause -> TableName -> [Int] -> Either ErrorMessage DataFrame
-- selectColumnsFromDataFrame whereCondition tableName columnIndices = do
--     let realCols = columns (executeWhere whereCondition tableName)
--         realRows = getDataFrameRows (executeWhere whereCondition tableName)
--         selectedColumns = map (realCols !!) columnIndices
--         selectedRows = map (\row -> map (row !!) columnIndices) realRows
--     Right $ DataFrame selectedColumns selectedRows

-- -- remake
-- selectSpecifiedColumnsFromTable :: TableName -> [String] -> Maybe WhereClause -> Either ErrorMessage DataFrame
-- selectSpecifiedColumnsFromTable tableName columnNames whereCondition =
--     case lookup tableName database of
--       Just df ->
--         case mapM (`findColumnIndex` df) columnNames of
--           Just columnIndices -> selectColumnsFromDataFrame whereCondition tableName columnIndices
--           Nothing -> Left $ "One or more columns not found in table " ++ tableName
--       Nothing -> Left $ "Table " ++ tableName ++ " not found"

-- averageOfIntValues' :: [Value] -> Value
-- averageOfIntValues' values
--   | null values || all isNullValue values = NullValue
--   | otherwise = IntegerValue avg
--   where
--     sumValues = sumIntValues values
--     avg = sumValues `div` fromIntegral (length values)

-- aggregateColumnToValue :: AggregateColumn -> DataFrame -> (Column, Value)
-- aggregateColumnToValue (AvgColumn columnName) dataFrame = (avgColumn, averageOfIntValues' validIntValues)
--   where
--     avgColumn = Column ("avg " ++ columnName) IntegerType
--     values = map (\row -> getColumnValue (findColumnIndexUnsafe columnName dataFrame) row) (getDataFrameRows dataFrame)
--     validIntValues = filter isIntegerValue values

-- aggregateColumnToValue (MaxColumn columnName) dataFrame =
--   case sqlMax dataFrame columnName of
--     Right value -> (maxColumn, value)
--     Left _ -> (maxColumn, NullValue)
--   where
--     maxColumn = Column ("max " ++ columnName) $ getColumnType $ getColumnByName columnName $ columns dataFrame

-- -- max aggregate function
-- sqlMax :: DataFrame -> String -> Either ErrorMessage Value
-- sqlMax df col
--   | col `elem` getColNameList cols && isRightValue (getColumnByName col cols) = Right (maximum'' columnValues)
--   | otherwise = Left "Cannot get max of this value type or table does not exist"
--   where
--     cols = columns df
--     columnValues = getValues (getDataFrameRows df) (fromMaybe 0 (elemIndex col (getColNameList (columns df))))

--     isRightValue :: Column -> Bool
--     isRightValue (Column _ valueType) = valueType == IntegerType || valueType == StringType || valueType == BoolType

--     maximum'' :: [Value] -> Value
--     maximum'' [x] = x
--     maximum'' (x : x' : xs) = maximum'' ((if compValue x x' then x else x') : xs)
--     maximum'' _ = NullValue

--     compValue :: Value -> Value -> Bool
--     compValue (IntegerValue val1) (IntegerValue val2) = val1 > val2
--     compValue (StringValue val1) (StringValue val2) = val1 > val2
--     compValue (BoolValue val1) (BoolValue val2) = val1 > val2
--     compValue (IntegerValue _) NullValue = True
--     compValue (StringValue _) NullValue = True
--     compValue (BoolValue _) NullValue = True
--     compValue NullValue (IntegerValue _) = False
--     compValue NullValue (StringValue _) = False
--     compValue NullValue (BoolValue _) = False
--     compValue _ _ = True

-- -- Util functions

-- -- remake
-- columnNameExists :: TableName -> String -> Bool
-- columnNameExists tableName columnName =
--   case lookup tableName database of
--     Just df ->
--       any (\(Column name _) -> name == columnName) (columns df)
--     Nothing -> False

-- -- unsafe
-- getColumnByName :: String -> [Column] -> Column
-- getColumnByName name cols = fromMaybe (Column "notfound" BoolType) (find (\(Column colName _) -> colName == name) cols)

-- getColNameList :: [Column] -> [String]
-- getColNameList = map (\(Column name _) -> name)

-- getColumnType :: Column -> ColumnType
-- getColumnType (Column _ columnType) = columnType

-- -- remake
-- getDataFrameByName :: TableName -> DataFrame
-- getDataFrameByName name = fromMaybe (DataFrame [] []) (lookup name database)

-- getDataFrameRows :: DataFrame -> [Row]
-- getDataFrameRows (DataFrame _ rows) = rows

-- -- remake
-- isTableInDatabase :: TableName -> Bool
-- isTableInDatabase name = case lookup name database of
--   Just _ -> True
--   Nothing -> False

-- findColumnIndex :: String -> DataFrame -> Maybe Int
-- findColumnIndex columnName (DataFrame cols _) =
--   elemIndex columnName (map (\(Column name _) -> name) cols)

-- -- unsafe
-- findColumnIndexUnsafe :: String -> DataFrame -> Int
-- findColumnIndexUnsafe columnName (DataFrame cols _) =
--   fromMaybe 1 (elemIndex columnName (map (\(Column name _) -> name) cols))



-- getColumnValue :: Int -> Row -> Value
-- getColumnValue columnIndex row = row !! columnIndex

-- isIntegerValue :: Value -> Bool
-- isIntegerValue (IntegerValue _) = True
-- isIntegerValue _ = False

-- sumIntValues :: [Value] -> Integer
-- sumIntValues = foldr (\(IntegerValue x) acc -> x + acc) 0

-- isNullValue :: Value -> Bool
-- isNullValue NullValue = True
-- isNullValue _ = False

-- columns :: DataFrame -> [Column]
-- columns (DataFrame cols _) = cols

-- isNumber :: String -> Bool
-- isNumber "" = False
-- isNumber xs =
--   case dropWhile isDigit xs of
--     "" -> True
--     _ -> False

-- compareMaybe :: Eq a => a -> Maybe a -> Bool
-- compareMaybe val1 (Just val2) = val1 == val2
-- compareMaybe _ Nothing = False

-- getValues :: [Row] -> Int -> [Value]
-- getValues rows index = map (!! index) rows