/*
 * Copyright 2014 Databricks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.databricks.spark.xml;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

@SuppressWarnings("unused")
public final class JavaXmlSuite {

    private static final int numBooks = 12;
    private static final String booksFile = "src/test/resources/books.xml";
    private static final String booksFileTag = "book";
    private static final String personFullFile = "src/test/resources/person_full.xml";
    private static final String personPartialFile = "src/test/resources/person_partial.xml";
    private static final String personXSDFile = "src/test/resources/person.xsd";
    private static final String personTag = "person";
    private static final int numPersonElements = 2;
    private static final String studentFullFile = "src/test/resources/student_full.xml";
    private static final String studentPartialFile = "src/test/resources/student_partial.xml";
    private static final String studentXSDFile = "src/test/resources/student.xsd";
    private static final String studentTag = "student";
    private static final int numStudents = 2;

    private SparkSession spark;
    private Path tempDir;

    @Before
    public void setUp() throws IOException {
        spark = SparkSession.builder().
            master("local[2]").
            appName("XmlSuite").
            config("spark.ui.enabled", false).
            getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        tempDir = Files.createTempDirectory("JavaXmlSuite");
        tempDir.toFile().deleteOnExit();
    }

    @After
    public void tearDown() {
        spark.stop();
        spark = null;
    }

    private Path getEmptyTempDir() throws IOException {
        return Files.createTempDirectory(tempDir, "test");
    }

    @Test
    public void testXmlParser() {
        Dataset<Row> df = (new XmlReader()).withRowTag(booksFileTag).xmlFile(spark, booksFile);
        String prefix = XmlOptions.DEFAULT_ATTRIBUTE_PREFIX();
        long result = df.select(prefix + "id").count();
        Assert.assertEquals(result, numBooks);
    }

    // Test schemaXSDFile option
    @Test
    public void testXmlParserWithXSDFile_4AllDataElements() {
        Map<String, String> options = new HashMap<>();
        options.put("rowTag", personTag);
        options.put("xsdFile", personXSDFile);
        Dataset<Row> df = spark.read().options(options).format("xml").load(personFullFile);
        Assert.assertEquals(numPersonElements, df.count());
        df.collectAsList().forEach(System.out::println);
        Assert.assertEquals(2, df.schema().fields().length);
        Assert.assertEquals("string", df.schema().fields()[0].dataType().simpleString());
        Assert.assertEquals("struct<last_name:string,first_name:string>", df.schema().fields()[1].dataType().simpleString());
    }

    @Test(expected = com.databricks.spark.xml.util.XSDParsingException.class)
    public void testXmlParserWithXSDFile_RowTagMissing() {
        Map<String, String> options = new HashMap<>();
        options.put("rowTag", studentTag);
        options.put("xsdFile", personXSDFile);
        Dataset<Row> df = spark.read().options(options).format("xml").load(personFullFile);
    }

    // Test schemaXSDFile option, with partial structure
    @Test
    public void testXmlParserWithXSDFile_4PartialDataElements() {
        Map<String, String> options = new HashMap<>();
        options.put("rowTag", personTag);
        Dataset<Row> df = spark.read().options(options).format("xml").load(personPartialFile);
        Assert.assertEquals(df.count(), numPersonElements);
        df.collectAsList().forEach(System.out::println);
        // without XSD/schema, it will have one field only
        Assert.assertEquals(1, df.schema().fields().length);
        Assert.assertEquals("bigint", df.schema().fields()[0].dataType().simpleString());

        // with XSD/schema it will have two fields
        options.put("xsdFile", personXSDFile);
        df = spark.read().options(options).format("xml").load(personPartialFile);
        Assert.assertEquals(2, df.schema().fields().length);
        Assert.assertEquals("string", df.schema().fields()[0].dataType().simpleString());
        Assert.assertEquals("struct<last_name:string,first_name:string>", df.schema().fields()[1].dataType().simpleString());
    }

    // Test without schemaXSDFile option, two batches of data sets; one set of data having empty structType
    // without schema missing structType interpreted as a empty string; causing merging both batches will
    // result in exception - incompatible data types
    @Test(expected = org.apache.spark.sql.AnalysisException.class)
    public void testXmlParserMultipleFiles_Fail() {
        Map<String, String> options = new HashMap<>();
        options.put("rowTag", studentTag);
        options.put("treatEmptyValuesAsNulls", "true"); // makes no difference with or without it, for the current test
        Dataset<Row> df = spark.read().options(options).format("xml").load(studentFullFile);
        Assert.assertEquals(df.count(), numStudents);
        df.collectAsList().forEach(System.out::println);
        Dataset<Row> df1 = spark.read().options(options).format("xml").load(studentPartialFile);
        df1.collectAsList().forEach(System.out::println);
        Dataset<Row> df_f = df1.union(df);
    }

    // Test without schemaXSDFile option, two batches of data sets; one set of data having empty structType
    // without schema missing structType interpreted as a empty string; causing merging both batches will
    // result in exception - incompatible data types
    @Test
    public void testXmlParserMultipleFiles_Success() {
        Map<String, String> options = new HashMap<>();
        options.put("rowTag", studentTag);
        options.put("treatEmptyValuesAsNulls", "true"); // makes no difference with or without it, for the current test
        options.put("xsdFile", studentXSDFile);
        Dataset<Row> df = spark.read().options(options).format("xml").load(studentFullFile);
        Assert.assertEquals(df.count(), numStudents);
        df.collectAsList().forEach(System.out::println);
        Dataset<Row> df1 = spark.read().options(options).format("xml").load(studentPartialFile);
        df1.collectAsList().forEach(System.out::println);
        Dataset<Row> df_fnl = df1.union(df);
        df_fnl.collectAsList().forEach(System.out::println);
    }

    @Test
    public void testLoad() {
        Map<String, String> options = new HashMap<>();
        options.put("rowTag", booksFileTag);
        Dataset<Row> df = spark.read().options(options).format("xml").load(booksFile);
        long result = df.select("description").count();
        Assert.assertEquals(result, numBooks);
    }

    @Test
    public void testSave() throws IOException {
        Path booksPath = getEmptyTempDir().resolve("booksFile");

        Dataset<Row> df = (new XmlReader()).withRowTag(booksFileTag).xmlFile(spark, booksFile);
        df.select("price", "description").write().format("xml").save(booksPath.toString());

        Dataset<Row> newDf = (new XmlReader()).xmlFile(spark, booksPath.toString());
        long result = newDf.select("price").count();
        Assert.assertEquals(result, numBooks);
    }

}
