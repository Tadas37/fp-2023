{-# OPTIONS_GHC -Wno-incomplete-patterns #-}
{-# OPTIONS_GHC -Wno-incomplete-uni-patterns #-}
{-# OPTIONS_GHC -Wno-overlapping-patterns #-}
{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib2
  ( parseStatement,
    executeStatement,
    filterRowsByBoolColumn,
    sqlMax,
    WhereClause (..),
    ParsedStatement (..),
    Condition (..),
    ConditionValue (..),
  )
where

import Data.Char (toLower, isDigit)
import Data.List (elemIndex, find, isPrefixOf, isSuffixOf)
import Data.Maybe (fromMaybe)
import DataFrame (Column (..), ColumnType (..), DataFrame (..), Row, Value (..))
import InMemoryTables (TableName, database)

type ErrorMessage = String

data ParsedStatement
  = ShowTable TableName
  | ShowTables
  | SelectAll TableName (Maybe WhereClause)
  | SelectColumns TableName [String] (Maybe WhereClause)
  | AggregateSelect TableName [AggregateColumn] (Maybe WhereClause)
  deriving (Show, Eq)

data AllColumns = AllColumns
  deriving (Show, Eq)

data WhereClause
  = IsValueBool Bool TableName String
  | Conditions [Condition]
  deriving (Show, Eq)

data AggregateColumn
  = MaxColumn String
  | AvgColumn String
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

parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement input
  | last input /= ';' = Left "Unsupported or invalid statement"
  | otherwise = mapStatementType wordsInput
  where
    cleanedInput = init input
    wordsInput = parseSemiCaseSensitive cleanedInput

mapStatementType :: [String] -> Either ErrorMessage ParsedStatement
mapStatementType statement = case statement of
  ["show", "table", tableName] ->
    if tableNameExists tableName
      then Right (ShowTable tableName)
      else Left "Table not found"
  ["show", "tables"] -> Right ShowTables
  "select" : rest -> parseSelect rest
  _ -> Left "Unsupported or invalid statement"

parseSelect :: [String] -> Either ErrorMessage ParsedStatement
parseSelect statement = parseFunctionBody
  where
    (_, afterWhere) = break (== "where") statement
    (_, afterIs) = break (== "is") afterWhere
    (columnWords, fromAndWhere) = break (== "from") statement

    wordsAfterWhere = length afterWhere
    hasWhereClause = wordsAfterWhere > 0
    isBoolIsTrueFalseClauseLike = hasWhereClause && length afterIs == 2
    isAndClauseLike = hasWhereClause && null afterIs

    statementClause :: Either ErrorMessage (Maybe WhereClause)
    statementClause
      | not hasWhereClause && length fromAndWhere == 2 = Right Nothing
      | isBoolIsTrueFalseClauseLike = case parseWhereBoolIsTrueFalse fromAndWhere of
        Left err -> Left err
        Right clause -> Right $ Just clause
      | isAndClauseLike = case parseWhereAnd fromAndWhere of
        Left err -> Left err
        Right clause -> Right $ Just clause
      | otherwise = Left "Unsupported or invalid statement"

    columnName = drop 4 $ init (head columnWords)

    tableName
      | length fromAndWhere >= 2 && head fromAndWhere == "from" = fromAndWhere !! 1
      | otherwise = ""

    columnString = unwords columnWords
    columnNames = map (dropWhile (== ' ')) $ splitByComma columnString

    parseFunctionBody :: Either ErrorMessage ParsedStatement
    parseFunctionBody = case statementClause of
      Left err -> Left err
      Right clause
        | not (null columnNames) && listConsistsOfValidAggregates columnNames tableName -> Right $ AggregateSelect tableName (map parseAggregate columnNames) clause
        | length columnWords == 1 && head columnWords == "*"-> Right (SelectAll tableName clause)
        | not (null columnNames) && all (columnNameExists tableName) columnNames -> Right (SelectColumns tableName columnNames clause)
        | otherwise -> Left "Unsupported or invalid statement"

listConsistsOfValidAggregates :: [String] -> TableName -> Bool
listConsistsOfValidAggregates [] _ = True
listConsistsOfValidAggregates [columnName] tableName
  | "avg(" `isPrefixOf` columnName && ")" `isSuffixOf` columnName && isColumn && columnIsInt = True
  | "max(" `isPrefixOf` columnName && ")" `isSuffixOf` columnName && isColumn = True
  | otherwise = False
  where
    selectedColumn = init (drop 4 columnName)
    isColumn = columnNameExists tableName selectedColumn
    columnIsInt = getColumnType (getColumnByName selectedColumn (columns (getDataFrameByName tableName))) == IntegerType
listConsistsOfValidAggregates (x : xs) tableName = listConsistsOfValidAggregates [x] tableName && listConsistsOfValidAggregates xs tableName

parseAggregate :: String -> AggregateColumn
parseAggregate columnName
  | "avg" `isPrefixOf` columnName = AvgColumn $ drop 4 $ init columnName
  | "max" `isPrefixOf` columnName = MaxColumn $ drop 4 $ init columnName

parseWhereBoolIsTrueFalse :: [String] ->Either ErrorMessage WhereClause
parseWhereBoolIsTrueFalse fromAndWhere
  | matchesWhereBoolTrueFalsePatern && isValidWhereClause = splitStatementToWhereClause fromAndWhere
  | otherwise = Left "Unsupported or invalid statement"
  where
    matchesWhereBoolTrueFalsePatern = length fromAndWhere == 6 && head fromAndWhere == "from" && fromAndWhere !! 2 == "where" && fromAndWhere !! 4 == "is" && (fromAndWhere !! 5 == "false" || fromAndWhere !! 5 == "true" )
    tableName = fromAndWhere !! 1
    columnName = fromAndWhere !! 3
    tableColumns = columns (getDataFrameByName tableName)
    columnIsBool = getColumnType (getColumnByName columnName tableColumns) == BoolType
    isValidWhereClause = columnNameExists tableName columnName && columnIsBool

parseWhereAnd :: [String] -> Either ErrorMessage WhereClause
parseWhereAnd fromAndWhere
  | matchesWhereAndPattern (drop 3 fromAndWhere) (fromAndWhere !! 1) = splitStatementToAndClause (drop 3 fromAndWhere) (fromAndWhere !! 1)
  | otherwise = Left "Unsupported or invalid statement"
  where
    splitStatementToAndClause :: [String] -> TableName -> Either ErrorMessage WhereClause
    splitStatementToAndClause strList tableName = Right (Conditions (getConditionList strList tableName))
    splitStatementToAndClause _ _ = Left "Unsupported or invalid statement"

    getConditionList :: [String] -> TableName -> [Condition]
    getConditionList [condition1, operator, condition2] tableName = [getCondition condition1 operator condition2 tableName]
    getConditionList (condition1 : operator : condition2 : _ : xs) tableName = getCondition condition1 operator condition2 tableName : getConditionList xs tableName

getConditionValue :: String -> ConditionValue
getConditionValue condition
  | isNumber condition = IntValue (read condition :: Integer)
  | length condition > 2 && "'" `isPrefixOf` condition && "'" `isSuffixOf` condition = StrValue (drop 1 (init condition))

getCondition :: String -> String -> String -> String -> Condition
getCondition val1 op val2 tableName
  | val1IsColumn && op == "=" && col1MatchesVal2 = Equals val1 $ getConditionValue val2
  | val2IsColumn && op == "=" && col2MatchesVal1 = Equals val2 $ getConditionValue val1
  | val1IsColumn && op == "<" && col1MatchesVal2 = LessThan val1 $ getConditionValue val2
  | val2IsColumn && op == "<" && col2MatchesVal1 = LessThan val2 $ getConditionValue val1
  | val1IsColumn && op == ">" && col1MatchesVal2 = GreaterThan val1 $ getConditionValue val2
  | val2IsColumn && op == ">" && col2MatchesVal1 = GreaterThan val2 $ getConditionValue val1
  | val1IsColumn && op == ">=" && col1MatchesVal2 = GreaterThanOrEqual val1 $ getConditionValue val2
  | val2IsColumn && op == ">=" && col2MatchesVal1 = GreaterThanOrEqual val2 $ getConditionValue val1
  | val1IsColumn && op == "<=" && col1MatchesVal2 = LessthanOrEqual val1 $ getConditionValue val2
  | val2IsColumn && op == "<=" && col2MatchesVal1 = LessthanOrEqual val2 $ getConditionValue val1
  | val1IsColumn && op == "<>" && col1MatchesVal2 = NotEqual val1 $ getConditionValue val2
  | val2IsColumn && op == "<>" && col2MatchesVal1 = NotEqual val2 $ getConditionValue val1

  where
    val1IsColumn = columnNameExists tableName val1
    val2IsColumn = columnNameExists tableName val2
    df = getDataFrameByName tableName
    val1Column = getColumnByName val1 (columns df)
    val2Column = getColumnByName val2 (columns df)
    col1MatchesVal2 = compareMaybe (getColumnType val1Column) (parseType val2)
    col2MatchesVal1 = compareMaybe (getColumnType val2Column) (parseType val1)

matchesWhereAndPattern :: [String] -> TableName -> Bool
matchesWhereAndPattern [condition1, operator, condition2] tableName = isWhereAndOperation [condition1, operator, condition2] tableName
matchesWhereAndPattern (condition1 : operator: condition2 : andString: xs) tableName = matchesWhereAndPattern [condition1, operator, condition2] tableName && andString == "and" && matchesWhereAndPattern xs tableName
matchesWhereAndPattern _ _ = False

isWhereAndOperation :: [String] -> TableName -> Bool
isWhereAndOperation [condition1, operator, condition2] tableName
  | columnNameExists tableName condition1 && elem operator [">", "<", "=", "<>", "<=", ">="] && col1MatchesVal2 = True
  | columnNameExists tableName condition2 && elem operator [">", "<", "=", "<>", "<=", ">="] && col2MatchesVal1 = True
  | otherwise = False
  where
    df = getDataFrameByName tableName
    val1Column = getColumnByName condition1 (columns df)
    val2Column = getColumnByName condition2 (columns df)
    col1MatchesVal2 = compareMaybe (getColumnType val1Column) (parseType condition2)
    col2MatchesVal1 = compareMaybe (getColumnType val2Column) (parseType condition1)
isWhereAndOperation _ _ = False

parseType :: String -> Maybe ColumnType
parseType str
  | isNumber str = Just IntegerType
  | "'" `isPrefixOf` str && "'" `isSuffixOf` str = Just StringType
  | otherwise = Nothing


splitStatementToWhereClause :: [String] -> Either ErrorMessage WhereClause
splitStatementToWhereClause ["from", tableName, "where", boolColName, "is", boolString] = Right $ IsValueBool parsedBoolString tableName boolColName
  where
    parsedBoolString = boolString == "true"
splitStatementToWhereClause _ = Left "Unsupported or invalid statement"

selectAllFromTable :: TableName -> Maybe WhereClause -> Either ErrorMessage DataFrame
selectAllFromTable tableName whereCondition =
    case lookup tableName database of
        Just _ -> Right (executeWhere whereCondition tableName)
        Nothing -> Left $ "Table " ++ tableName ++ " not found"

splitByComma :: String -> [String]
splitByComma = map (dropWhile (== ' ')) . words . map (\c -> if c == ',' then ' ' else c)

parseSemiCaseSensitive :: String -> [String]
parseSemiCaseSensitive statement = convertedStatement
  where
    splitStatement = words statement
    convertedStatement = map wordToLowerSensitive splitStatement

wordToLowerSensitive :: String -> String
wordToLowerSensitive word
  | map toLower word `elem` keywords = map toLower word
  | "avg(" `isPrefixOf` map toLower word && ")" `isSuffixOf` word = "avg(" ++ drop 4 (init word) ++ ")"
  | "max(" `isPrefixOf` map toLower word && ")" `isSuffixOf` word = "max(" ++ drop 4 (init word) ++ ")"
  | otherwise = word
  where
    keywords = ["select", "from", "where", "show", "table", "tables", "false", "true", "and", "is"]

tableNameExists :: TableName -> Bool
tableNameExists name = any (\(tableName, _) -> tableName == name) database

executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement (ShowTable tableName) =
  case lookup tableName database of
    Just df -> Right $ DataFrame [Column "Columns" StringType] (map (\(Column name _) -> [StringValue name]) (columns df))
    Nothing -> Left $ "Table " ++ tableName ++ " not found"
executeStatement ShowTables =
  Right $ DataFrame [Column "Tables" StringType] (map (\(name, _) -> [StringValue name]) database)
executeStatement (SelectColumns tableName columnNames whereCondition) = selectSpecifiedColumnsFromTable tableName columnNames whereCondition
executeStatement (AggregateSelect tableName aggregates whereCondition) = Right $ DataFrame parsedColumns [parsedRow]
  where
    df = executeWhere whereCondition tableName
    listAggregateColumns = map (`aggregateColumnToValue` df) aggregates
    parsedColumns = map fst listAggregateColumns
    parsedRow = map snd listAggregateColumns

-- executeStatement (AvgColumn tableName columnName whereCondition) = calculateAverageFromTableColumn tableName columnName whereCondition
-- executeStatement (MaxColumn tableName columnName whereCondition) = case sqlMax (executeWhere whereCondition tableName) columnName of
  -- Right value -> Right (DataFrame [getColumnByName columnName (columns (getDataFrameByName tableName))] [[value]])
  -- Left msg -> Left msg
executeStatement (SelectAll tableName whereCondition) =
  Right $ executeWhere whereCondition tableName

executeWhere :: Maybe WhereClause -> TableName -> DataFrame
executeWhere whereClause tableName =
    case whereClause of
        Just (IsValueBool bool table column) ->
            case filterRowsByBoolColumn table column bool of
                Right df -> df
                Left _   -> getDataFrameByName tableName

        Just (Conditions conditions) ->
            case filterRowsByConditions tableName conditions of
                Right df -> df
                Left _   -> getDataFrameByName tableName

        Nothing ->
            getDataFrameByName tableName

-- Filter rows based on whether the specified column's value is TRUE or FALSE.
filterRowsByBoolColumn :: TableName -> String -> Bool -> Either ErrorMessage DataFrame
filterRowsByBoolColumn name col bool
  | not $ isTableInDatabase name                      = Left combinedError
  | not $ col `elem` columnNames                      = Left combinedError
  | getColumnType currentColumn /= BoolType           = Left combinedError
  | otherwise                                         = Right $ getRowsByBool bool currentRows
  where
    currentDataFrame = getDataFrameByName name
    currentColumns   = columns currentDataFrame
    columnNames      = getColNameList currentColumns
    currentRows      = getDataFrameRows currentDataFrame
    currentColumn    = getColumnByName col currentColumns

    combinedError = "Dataframe does not exist or does not contain column by specified name or column is not of type bool"

    getRowsByBool :: Bool -> [Row] -> DataFrame
    getRowsByBool boolValue rows = DataFrame currentColumns $ filter (matchesBool boolValue) rows

    matchesBool :: Bool -> Row -> Bool
    matchesBool boolVal row = case columnIndex of
      Just ind -> row !! ind == BoolValue boolVal
      Nothing  -> False

    columnIndex :: Maybe Int
    columnIndex = elemIndex col columnNames

filterRowsByConditions :: TableName -> [Condition] -> Either ErrorMessage DataFrame
filterRowsByConditions name conditions
  | not $ isTableInDatabase name = Left "Table does not exist."
  | otherwise = Right $ DataFrame currentColumns $ filter (matchesConditions conditions) currentRows
  where
    currentDataFrame = getDataFrameByName name
    currentColumns   = columns currentDataFrame
    currentRows      = getDataFrameRows currentDataFrame

    matchesConditions :: [Condition] -> Row -> Bool
    matchesConditions [] _ = True
    matchesConditions (c:cs) row = evaluateCondition c row && matchesConditions cs row

    evaluateCondition :: Condition -> Row -> Bool
    evaluateCondition (Equals colName (StrValue val)) row =
        getValueByColumnName colName row (getColNameList currentColumns) == StringValue val

    evaluateCondition (Equals colName (IntValue val)) row =
        case getValueByColumnName colName row (getColNameList currentColumns) of
            IntegerValue intVal -> intVal == val
            _ -> False

    evaluateCondition (GreaterThan colName (StrValue val)) row =
        case getValueByColumnName colName row (getColNameList currentColumns) of
            StringValue strVal -> strVal > val
            _ -> False

    evaluateCondition (GreaterThan colName (IntValue val)) row =
        case getValueByColumnName colName row (getColNameList currentColumns) of
            IntegerValue intVal -> intVal > val
            _ -> False

    evaluateCondition (LessThan colName (StrValue val)) row =
        case getValueByColumnName colName row (getColNameList currentColumns) of
            StringValue strVal -> strVal < val
            _ -> False

    evaluateCondition (LessThan colName (IntValue val)) row =
        case getValueByColumnName colName row (getColNameList currentColumns) of
            IntegerValue intVal -> intVal < val
            _ -> False

    evaluateCondition (GreaterThanOrEqual colName (StrValue val)) row =
        case getValueByColumnName colName row (getColNameList currentColumns) of
            StringValue strVal -> strVal >= val
            _ -> False

    evaluateCondition (GreaterThanOrEqual colName (IntValue val)) row =
        case getValueByColumnName colName row (getColNameList currentColumns) of
            IntegerValue intVal -> intVal >= val
            _ -> False

    evaluateCondition (LessthanOrEqual colName (StrValue val)) row =
        case getValueByColumnName colName row (getColNameList currentColumns) of
            StringValue strVal -> strVal <= val
            _ -> False

    evaluateCondition (LessthanOrEqual colName (IntValue val)) row =
        case getValueByColumnName colName row (getColNameList currentColumns) of
            IntegerValue intVal -> intVal <= val
            _ -> False

    evaluateCondition (NotEqual colName (StrValue val)) row =
        getValueByColumnName colName row (getColNameList currentColumns) /= StringValue val

    evaluateCondition (NotEqual colName (IntValue val)) row =
        case getValueByColumnName colName row (getColNameList currentColumns) of
            IntegerValue intVal -> intVal /= val
            _ -> False

    getValueByColumnName :: String -> Row -> [String] -> Value
    getValueByColumnName colName row columnNames =
      case elemIndex colName columnNames of
        Just ind -> row !! ind
        Nothing  -> NullValue

--selectColumns
selectColumnsFromDataFrame :: Maybe WhereClause -> TableName -> [Int] -> Either ErrorMessage DataFrame
selectColumnsFromDataFrame whereCondition tableName columnIndices = do
    let realCols = columns (executeWhere whereCondition tableName)
        realRows = getDataFrameRows (executeWhere whereCondition tableName)
        selectedColumns = map (realCols !!) columnIndices
        selectedRows = map (\row -> map (row !!) columnIndices) realRows
    Right $ DataFrame selectedColumns selectedRows

selectSpecifiedColumnsFromTable :: TableName -> [String] -> Maybe WhereClause -> Either ErrorMessage DataFrame
selectSpecifiedColumnsFromTable tableName columnNames whereCondition =
    case lookup tableName database of
      Just df ->
        case mapM (`findColumnIndex` df) columnNames of
          Just columnIndices -> selectColumnsFromDataFrame whereCondition tableName columnIndices
          Nothing -> Left $ "One or more columns not found in table " ++ tableName
      Nothing -> Left $ "Table " ++ tableName ++ " not found"

--AVG agregate function
averageOfIntValues :: [Value] -> Either ErrorMessage DataFrame
averageOfIntValues validIntValues
  | null validIntValues = Left "No valid integers found in the specified column"
  | otherwise =
      let sumValues = sumIntValues validIntValues
          avg = fromIntegral sumValues / fromIntegral (length validIntValues)
      in Right $ DataFrame [Column "AVG" IntegerType] [[IntegerValue (round avg)]]

averageOfIntValues' :: [Value] -> Value
averageOfIntValues' values
  | null values || all isNullValue values = NullValue
  | otherwise = IntegerValue avg
  where
    sumValues = sumIntValues values
    avg = sumValues `div` fromIntegral (length values)

aggregateColumnToValue :: AggregateColumn -> DataFrame -> (Column, Value)
aggregateColumnToValue (AvgColumn columnName) dataFrame = (avgColumn, averageOfIntValues' validIntValues)
  where
    avgColumn = Column ("avg " ++ columnName) IntegerType
    values = map (\row -> getColumnValue (findColumnIndexUnsafe columnName dataFrame) row) (getDataFrameRows dataFrame)
    validIntValues = filter isIntegerValue values

aggregateColumnToValue (MaxColumn columnName) dataFrame = 
  case sqlMax dataFrame columnName of 
    Right value -> (maxColumn, value)
    Left _ -> (maxColumn, NullValue)
  where
    maxColumn = Column ("max " ++ columnName) $ getColumnType $ getColumnByName columnName $ columns dataFrame

calculateAverageFromTableColumn :: TableName -> String -> Maybe WhereClause -> Either ErrorMessage DataFrame
calculateAverageFromTableColumn tableName columnName whereCondition =
    case lookup tableName database of
      Just df ->
        case findColumnIndex columnName df of
          Just columnIndex ->
            let values = map (\row -> getColumnValue columnIndex row) (getDataFrameRows (executeWhere whereCondition tableName))
                validIntValues = filter isIntegerValue values
            in if null validIntValues
              then Left "No valid integers found in the specified column"
              else averageOfIntValues validIntValues
          Nothing -> Left $ "Column " ++ columnName ++ " not found in table " ++ tableName
      Nothing -> Left $ "Table " ++ tableName ++ " not found"


-- max aggregate function
sqlMax :: DataFrame -> String -> Either ErrorMessage Value
sqlMax df col
  | col `elem` getColNameList cols && isRightValue (getColumnByName col cols) = Right (maximum'' columnValues)
  | otherwise = Left "Cannot get max of this value type or table does not exist"
  where
    cols = columns df
    columnValues = getValues (getDataFrameRows df) (fromMaybe 0 (elemIndex col (getColNameList (columns df))))

    isRightValue :: Column -> Bool
    isRightValue (Column _ valueType) = valueType == IntegerType || valueType == StringType || valueType == BoolType

    maximum'' :: [Value] -> Value
    maximum'' [x] = x
    maximum'' (x : x' : xs) = maximum'' ((if compValue x x' then x else x') : xs)
    maximum'' _ = NullValue

    compValue :: Value -> Value -> Bool
    compValue (IntegerValue val1) (IntegerValue val2) = val1 > val2
    compValue (StringValue val1) (StringValue val2) = val1 > val2
    compValue (BoolValue val1) (BoolValue val2) = val1 > val2
    compValue (IntegerValue _) NullValue = True
    compValue (StringValue _) NullValue = True
    compValue (BoolValue _) NullValue = True
    compValue NullValue (IntegerValue _) = False
    compValue NullValue (StringValue _) = False
    compValue NullValue (BoolValue _) = False
    compValue _ _ = True

-- Util functions
columnNameExists :: TableName -> String -> Bool
columnNameExists tableName columnName =
  case lookup tableName database of
    Just df ->
      any (\(Column name _) -> name == columnName) (columns df)
    Nothing -> False

getColumnByName :: String -> [Column] -> Column
getColumnByName name cols = fromMaybe (Column "notfound" BoolType) (find (\(Column colName _) -> colName == name) cols)

getColNameList :: [Column] -> [String]
getColNameList = map (\(Column name _) -> name)

getColumnType :: Column -> ColumnType
getColumnType (Column _ columnType) = columnType

getDataFrameByName :: TableName -> DataFrame
getDataFrameByName name = fromMaybe (DataFrame [] []) (lookup name database)

getDataFrameRows :: DataFrame -> [Row]
getDataFrameRows (DataFrame _ rows) = rows

isTableInDatabase :: TableName -> Bool
isTableInDatabase name = case lookup name database of
  Just _ -> True
  Nothing -> False

findColumnIndex :: String -> DataFrame -> Maybe Int
findColumnIndex columnName (DataFrame cols _) =
  elemIndex columnName (map (\(Column name _) -> name) cols)

findColumnIndexUnsafe :: String -> DataFrame -> Int
findColumnIndexUnsafe columnName (DataFrame cols _) =
  fromMaybe 1 (elemIndex columnName (map (\(Column name _) -> name) cols))



getColumnValue :: Int -> Row -> Value
getColumnValue columnIndex row = row !! columnIndex

isIntegerValue :: Value -> Bool
isIntegerValue (IntegerValue _) = True
isIntegerValue _ = False

sumIntValues :: [Value] -> Integer
sumIntValues = foldr (\(IntegerValue x) acc -> x + acc) 0

isNullValue :: Value -> Bool
isNullValue NullValue = True
isNullValue _ = False

columns :: DataFrame -> [Column]
columns (DataFrame cols _) = cols

isNumber :: String -> Bool
isNumber "" = False
isNumber xs =
  case dropWhile isDigit xs of
    "" -> True
    _ -> False

compareMaybe :: Eq a => a -> Maybe a -> Bool
compareMaybe val1 (Just val2) = val1 == val2
compareMaybe _ Nothing = False

getValues :: [Row] -> Int -> [Value]
getValues rows index = map (!! index) rows