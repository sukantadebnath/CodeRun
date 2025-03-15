import java.io.*;
import java.sql.*;
import java.util.Properties;

public class FetchDataToFile {

    // Main method to fetch data from the database and write to a file
    public static String getData(String configFilePath) {
        // Load properties from the provided configuration file
        Properties properties = loadProperties(configFilePath);
        if (properties == null) return null;

        // Load database properties from the specified path in the configuration
        Properties dbProperties = loadProperties(properties.getProperty("db.config.path"));
        if (dbProperties == null) return null;

        // Decrypt and retrieve database connection details
        String jdbcURL = decrypt(dbProperties.getProperty("jdbc.url"));
        String username = decrypt(dbProperties.getProperty("jdbc.username"));
        String password = decrypt(dbProperties.getProperty("jdbc.password"));
        String schema = dbProperties.getProperty("jdbc.schema");
        String executionType = properties.getProperty("execution.type");
        String sqlQuery = properties.getProperty("sql.query");
        String storedProcedureName = properties.getProperty("stored.procedure.name");
        String storedProcedureParams = properties.getProperty("stored.procedure.params");
        String fileNamePrefix = properties.getProperty("file.prefix");
        String delimiter = properties.getProperty("delimiter");
        String sqlParams = properties.getProperty("sql.params");

        // Validate the delimiter
        if (!isValidDelimiter(delimiter)) {
            System.out.println("Invalid delimiter. Please use ',' or '|'.");
            return null;
        }

        // Establish a connection to the database
        try (Connection connection = DriverManager.getConnection(jdbcURL, username, password)) {
            connection.setSchema(schema);
            // Execute the query or stored procedure based on the execution type
            ResultSet resultSet = executeQuery(connection, executionType, sqlQuery, storedProcedureName, storedProcedureParams, sqlParams);

            // Write the result set to a file if the query execution was successful
            if (resultSet != null) {
                return writeToFile(resultSet, fileNamePrefix, delimiter);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    // Load properties from a file
    private static Properties loadProperties(String filePath) {
        Properties properties = new Properties();
        try (InputStream input = new FileInputStream(filePath)) {
            properties.load(input);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        return properties;
    }

    // Validate if the delimiter is either ',' or '|'
    private static boolean isValidDelimiter(String delimiter) {
        return ",".equals(delimiter) || "|".equals(delimiter);
    }

    // Execute the query or stored procedure based on the execution type
    private static ResultSet executeQuery(Connection connection, String executionType, String sqlQuery, String storedProcedureName, String storedProcedureParams, String sqlParams) throws SQLException {
        if ("stored_procedure".equalsIgnoreCase(executionType)) {
            CallableStatement callableStatement = connection.prepareCall("{call " + storedProcedureName + "}");
            setParameters(callableStatement, storedProcedureParams.split(","), sqlParams.split(","));
            return callableStatement.executeQuery();
        } else {
            PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery);
            setParameters(preparedStatement, sqlParams.split(","));
            return preparedStatement.executeQuery();
        }
    }

    // Write the result set to a file
    private static String writeToFile(ResultSet resultSet, String fileNamePrefix, String delimiter) throws SQLException {
        String stagingOutputFileName = fileNamePrefix + ".txt";
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(stagingOutputFileName))) {
            // Write the header (column names) to the file
            writeHeader(resultSet, writer, delimiter);
            // Write the rows in batches to the file
            writeRowsInBatch(resultSet, writer, delimiter, 1000);
            System.out.println("Data exported successfully!");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return stagingOutputFileName;
    }

    // Write the header (column names) to the file
    private static void writeHeader(ResultSet resultSet, BufferedWriter writer, String delimiter) throws SQLException, IOException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            writer.write(metaData.getColumnName(i));
            if (i < columnCount) writer.write(delimiter);
        }
        writer.newLine();
    }

    // Write the rows in batches to the file
    private static void writeRowsInBatch(ResultSet resultSet, BufferedWriter writer, String delimiter, int batchSize) throws SQLException, IOException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        int rowCount = 0;
        StringBuilder batch = new StringBuilder();

        while (resultSet.next()) {
            for (int i = 1; i <= columnCount; i++) {
                batch.append(resultSet.getString(i));
                if (i < columnCount) batch.append(delimiter);
            }
            batch.append("\n");
            rowCount++;

            // Write the batch to the file when the batch size is reached
            if (rowCount % batchSize == 0) {
                writer.write(batch.toString());
                batch.setLength(0);
            }
        }

        // Write any remaining rows in the batch to the file
        if (batch.length() > 0) {
            writer.write(batch.toString());
        }
    }

    // Set parameters for the prepared statement
    private static void setParameters(PreparedStatement statement, String[] paramValues) throws SQLException {
        for (int i = 0; i < paramValues.length; i++) {
            statement.setString(i + 1, paramValues[i]);
        }
    }

    // Set parameters for the callable statement
    private static void setParameters(CallableStatement statement, String[] paramNames, String[] paramValues) throws SQLException {
        for (int i = 0; i < paramNames.length; i++) {
            statement.setString(paramNames[i], paramValues[i]);
        }
    }

    // Decrypt the encrypted value (placeholder method)
    private static String decrypt(String encryptedValue) {
        // To Do : Implement decryption logic
        return encryptedValue;
    }
}