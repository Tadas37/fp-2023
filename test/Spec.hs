import Data.Either
import Data.Maybe ()
import DataFrame (Column (..), ColumnType (..), DataFrame (..), Value (..))
import InMemoryTables qualified as D
import Lib1
import Lib2
import Test.Hspec

main :: IO ()
main = hspec $ do
  describe "Lib1.findTableByName" $ do
    it "handles empty lists" $ do
      Lib1.findTableByName [] "" `shouldBe` Nothing
    it "handles empty names" $ do
      Lib1.findTableByName D.database "" `shouldBe` Nothing
    it "can find by name" $ do
      Lib1.findTableByName D.database "employees" `shouldBe` Just (snd D.tableEmployees)
    it "can find by case-insensitive name" $ do
      Lib1.findTableByName D.database "employEEs" `shouldBe` Just (snd D.tableEmployees)

  describe "Lib1.parseSelectAllStatement" $ do
    it "handles empty input" $ do
      Lib1.parseSelectAllStatement "" `shouldSatisfy` isLeft
    it "handles invalid queries" $ do
      Lib1.parseSelectAllStatement "select from dual" `shouldSatisfy` isLeft
    it "returns table name from correct queries" $ do
      Lib1.parseSelectAllStatement "selecT * from dual;" `shouldBe` Right "dual"

  describe "Lib1.validateDataFrame" $ do
    it "finds types mismatch" $ do
      Lib1.validateDataFrame (snd D.tableInvalid1) `shouldSatisfy` isLeft
    it "finds column size mismatch" $ do
      Lib1.validateDataFrame (snd D.tableInvalid2) `shouldSatisfy` isLeft
    it "reports different error messages" $ do
      Lib1.validateDataFrame (snd D.tableInvalid1) `shouldNotBe` Lib1.validateDataFrame (snd D.tableInvalid2)
    it "passes valid tables" $ do
      Lib1.validateDataFrame (snd D.tableWithNulls) `shouldBe` Right ()

  describe "Lib1.renderDataFrameAsTable" $ do
    it "renders a table" $ do
      Lib1.renderDataFrameAsTable 100 (snd D.tableEmployees) `shouldSatisfy` not . null

  describe "filterRowsByBoolColumn" $ do
    it "should return list of matching rows" $ do
      filterRowsByBoolColumn (fst D.tableWithNulls) "value" True `shouldBe` Right (DataFrame [Column "flag" StringType, Column "value" BoolType] [[StringValue "a", BoolValue True], [StringValue "b", BoolValue True]])
      filterRowsByBoolColumn (fst D.tableWithNulls) "value" False `shouldBe` Right (DataFrame [Column "flag" StringType, Column "value" BoolType] [[StringValue "b", BoolValue False]])

    it "should return Error if Column is not bool type" $ do
      filterRowsByBoolColumn (fst D.tableWithNulls) "flag" True `shouldBe` Left "Dataframe does not exist or does not contain column by specified name or column is not of type bool"

    it "should return Error if Column is not in table" $ do
      filterRowsByBoolColumn (fst D.tableWithNulls) "flagz" True `shouldBe` Left "Dataframe does not exist or does not contain column by specified name or column is not of type bool"

  describe "sqlMax" $ do
    it "should return Left if column does not exist" $ do
      sqlMax (snd D.tableEmployees) "bonk" `shouldBe` Left "Cannot get max of this value type or table does not exist"

    it "should return max with correct parameters even if nulls in table" $ do
      sqlMax (snd D.tableEmployees) "id" `shouldBe` Right (IntegerValue 2)
      sqlMax (snd D.tableWithNulls) "value" `shouldBe` Right (BoolValue True)
      sqlMax (snd D.tableWithNulls) "flag" `shouldBe` Right (StringValue "b")

  describe "parseStatement in Lib2" $ do
    it "should parse 'SHOW TABLE employees;' correctly" $ do
      parseStatement "SHOW TABLE employees;" `shouldBe` Right (ShowTable "employees")

    it "should parse SELECT * FROM ..." $ do
      parseStatement "SELECT * FROM employees;" `shouldBe` Right (SelectAll "employees" Nothing)

    it "should return an error for malformed statements" $ do
      parseStatement "SHOW employees;" `shouldBe` Left "Unsupported or invalid statement"

    it "should return an error for statements without semicolon" $ do
      parseStatement "SHOW TABLE employees" `shouldBe` Left "Unsupported or invalid statement"

    it "should return an error for malformed AVG statements" $ do
      parseStatement "select AVG from employees;" `shouldBe` Left "Unsupported or invalid statement"

    it "should return an error for AVG statements without semicolon" $ do
      parseStatement "select AVG(id) from employees" `shouldBe` Left "Unsupported or invalid statement"

    it "should parse 'select AVG(id) from employees;' correctly with case-sensitive table and column names" $ do
      parseStatement "select AVG(id) from employees;" `shouldBe` Right (AvgColumn "employees" "id" Nothing)

    it "should not match incorrect case for table names" $ do
      parseStatement "select AVG(id) from EMPLOYEES;" `shouldBe` Left "Unsupported or invalid statement"

    it "should not match incorrect case for column names" $ do
      parseStatement "select AVG(iD) from employees;" `shouldBe` Left "Unsupported or invalid statement"

    it "should still match case-insensitive SQL keywords" $ do
      parseStatement "SELECT AVG(id) FROM employees;" `shouldBe` Right (AvgColumn "employees" "id" Nothing)

    it "should parse 'selEct MaX(id) From employees;' correctly with case-sensitive table and column names" $ do
      parseStatement "selEct MaX(id) From employees;" `shouldBe` Right (MaxColumn "employees" "id" Nothing)

    it "should parse 'selEct MaX(flag) From flags wheRe value iS tRue;'" $ do
      parseStatement "selEct MaX(flag) From flags wheRe value iS tRue;" `shouldBe` Right (MaxColumn "flags" "flag" (Just (IsValueBool True "flags" "value")))

    it "should parse 'SELECT column1 FROM employees;' correctly" $ do
      parseStatement "SELECT id FROM employees;" `shouldBe` Right (SelectColumns "employees" ["id"] Nothing)

    it "should parse 'SELECT column1, column2 FROM employees;' correctly" $ do
      parseStatement "SELECT id, name FROM employees;" `shouldBe` Right (SelectColumns "employees" ["id", "name"] Nothing)

    it "should parse 'SELECT name, avg(id) FROM employees;' as bad statement" $ do
      parseStatement "SELECT name, avg(id) FROM employees;" `shouldBe` Left "Unsupported or invalid statement"

    it "should return an error for statements without semicolon for SELECT" $ do
      parseStatement "SELECT id FROM employees" `shouldBe` Left "Unsupported or invalid statement"

    it "shouldn't parse 'selEct MaX(flag) From flags wheRe value iS tRuewad;'" $ do
      parseStatement "selEct MaX(flag) From flags wheRe value iS tRuewad;" `shouldBe` Left "Unsupported or invalid statement"
    
    it "shouldn't parse 'selEct MaX(flag) From flags wheRe valEZ iS tRuewad;'" $ do
      parseStatement "selEct MaX(flag) From flags wheRe value iS tRuewad;" `shouldBe` Left "Unsupported or invalid statement"

  describe "executeStatement in Lib2" $ do
    it "should list columns for 'SHOW TABLE employees;'" $ do
      let parsed = ShowTable "employees"
      let expectedColumns = [Column "Columns" StringType]
      let expectedRows = [[StringValue "id"], [StringValue "name"], [StringValue "surname"]]
      executeStatement parsed `shouldBe` Right (DataFrame expectedColumns expectedRows)

    it "should give an error for a non-existent table" $ do
      let parsed = ShowTable "nonexistent"
      executeStatement parsed `shouldBe` Left "Table nonexistent not found"

  describe "parseStatement for SHOW TABLES in Lib2" $ do
    it "should parse 'SHOW TABLES;' correctly" $ do
      parseStatement "SHOW TABLES;" `shouldBe` Right ShowTables

    it "should return an error for statements without semicolon for SHOW TABLES" $ do
      parseStatement "SHOW TABLES" `shouldBe` Left "Unsupported or invalid statement"

  describe "parseStatement for select... with where and" $ do
    it "should parse correct select statements with where and" $ do
      parseStatement "select max(flag) from flags where flag < 'b';" `shouldBe` Right (MaxColumn "flags" "flag" (Just (Conditions [LessThan "flag" (StrValue "b")])))
      parseStatement "select flag, value from flags where flag < 'c';" `shouldBe` Right (SelectColumns "flags" ["flag","value"] (Just (Conditions [LessThan "flag" (StrValue "c")])))
      parseStatement "select flag, value from flags where flag < 'c' and flag > 'c' and flag < 'd';" `shouldBe` Right (SelectColumns "flags" ["flag","value"] (Just (Conditions [LessThan "flag" (StrValue "c"),GreaterThan "flag" (StrValue "c"),LessThan "flag" (StrValue "d")])))
      parseStatement "selEct flag, value FRom flags wheRe flag < 'c' and flag > 'C' and flag = 'd';" `shouldBe` Right (SelectColumns "flags" ["flag","value"] (Just (Conditions [LessThan "flag" (StrValue "c"),GreaterThan "flag" (StrValue "C"),Equals "flag" (StrValue "d")])))

    it "shouldn't parse where and statements with mismatched and conditions e.g. int < string" $ do
      parseStatement "select value from flags where flag < 2;" `shouldBe` Left "Unsupported or invalid statement"
      parseStatement "select flag from flags where 2 < flag;" `shouldBe` Left "Unsupported or invalid statement"

    it "shouldn't parse statements with incomplete where and" $ do
      parseStatement "select id, name from employees where name < 'vi' and;" `shouldBe` Left "Unsupported or invalid statement"   
      parseStatement "select id, name from employees where name <;" `shouldBe` Left "Unsupported or invalid statement"   
      parseStatement "select id, name from employees where name;" `shouldBe` Left "Unsupported or invalid statement"  
      parseStatement "select id, name from employees wher;" `shouldBe` Left "Unsupported or invalid statement"  


  describe "executeStatement for SHOW TABLES in Lib2" $ do
    it "should list all tables for 'SHOW TABLES;'" $ do
      let expectedColumns = [Column "Tables" StringType]
      let expectedRows = map (\(name, _) -> [StringValue name]) D.database
      executeStatement ShowTables `shouldBe` Right (DataFrame expectedColumns expectedRows)

  describe "executeStatement for AvgColumn in Lib2" $ do
    it "should calculate the average of the 'id' column in 'employees'" $
      let parsed = AvgColumn "employees" "id" Nothing
          expectedValue = 2 -- Change the expected value to 2
       in executeStatement parsed `shouldBe` Right (DataFrame [Column "AVG" IntegerType] [[IntegerValue expectedValue]])

    it "should give an error for a non-existent table" $ do
      let parsed = AvgColumn "nonexistent" "id" Nothing
      executeStatement parsed `shouldBe` Left "Table nonexistent not found"

    it "should give an error for a non-existent column" $ do
      let parsed = AvgColumn "employees" "nonexistent_column" Nothing
      executeStatement parsed `shouldBe` Left "Column nonexistent_column not found in table employees"

    it "should return the 'id' column for 'SELECT id FROM employees;'" $ do
      let parsed = SelectColumns "employees" ["id"] Nothing
      let expectedColumns = [Column "id" IntegerType]
      let expectedRows = [[IntegerValue 1], [IntegerValue 2]]
      executeStatement parsed `shouldBe` Right (DataFrame expectedColumns expectedRows)

    it "should return the 'id' and 'name' columns for 'SELECT id, name FROM employees;'" $ do
      let parsed = SelectColumns "employees" ["id", "name"] Nothing
      let expectedColumns = [Column "id" IntegerType, Column "name" StringType]
      let expectedRows = [[IntegerValue 1, StringValue "Vi"], [IntegerValue 2, StringValue "Ed"]]
      executeStatement parsed `shouldBe` Right (DataFrame expectedColumns expectedRows)

    it "should return an error for a non-existent column in SELECT" $ do
      let parsed = SelectColumns "employees" ["id", "nonexistent_column"] Nothing
      executeStatement parsed `shouldBe` Left "One or more columns not found in table employees"