import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

public class ValueTranslation {

    // Error log file path
    private static final String ERROR_LOG_FILE = "translation_error.log";
    // Delimiters for splitting input and CSV files
    private static final String DELIMITER = "\\|";
    private static final String CSV_DELIMITER = ",";

    // Maps and sets to store translation data
    private static final Map<String, Map<String, String>> translationMap = new HashMap<>();
    private static final Set<String> fieldsToTranslate = new HashSet<>();
    private static final Map<String, String> sharedTranslationGroups = new HashMap<>();

    // Main method to translate values based on the configuration file
    public static void translateValues(String configFilePath) {
        Properties config = loadConfig(configFilePath);
        if (config == null) return;

        // Retrieve file paths and translation settings from config
        String inputFilePath = config.getProperty("inputFilePath");
        String translationFilePath = config.getProperty("translationFilePath");
        String fieldsToTranslateStr = config.getProperty("fieldsToTranslate");
        String sharedTranslationGroupsStr = config.getProperty("sharedTranslationGroups");
        String outputFilePath = config.getProperty("outputFilePath");

        // Load translation settings
        loadFieldsToTranslate(fieldsToTranslateStr);
        loadSharedTranslationGroups(sharedTranslationGroupsStr);
        loadTranslationMap(translationFilePath);

        // Process the input file and write to the output file
        processFile(inputFilePath, outputFilePath);
    }

    // Overloaded method to translate values based on the configuration file and input file
    public static void translateValues(String configFilePath, String inputFilePath) {
        Properties config = loadConfig(configFilePath);
        if (config == null) return;

        // Retrieve translation settings from config
        String translationFilePath = config.getProperty("translationFilePath");
        String fieldsToTranslateStr = config.getProperty("fieldsToTranslate");
        String sharedTranslationGroupsStr = config.getProperty("sharedTranslationGroups");
        String outputFilePath = config.getProperty("outputFilePath");

        // Load translation settings
        loadFieldsToTranslate(fieldsToTranslateStr);
        loadSharedTranslationGroups(sharedTranslationGroupsStr);
        loadTranslationMap(translationFilePath);

        // Process the input file and write to the output file
        processFile(inputFilePath, outputFilePath);
    }

    // Load configuration properties from a file
    private static Properties loadConfig(String configFilePath) {
        Properties config = new Properties();
        try (InputStream input = new FileInputStream(configFilePath)) {
            config.load(input);
        } catch (IOException e) {
            logError("Error loading config file: " + e.getMessage());
            return null;
        }
        return config;
    }

    // Load fields to translate from a comma-separated string
    private static void loadFieldsToTranslate(String fieldsToTranslateStr) {
        if (fieldsToTranslateStr != null && !fieldsToTranslateStr.isEmpty()) {
            fieldsToTranslate.addAll(Arrays.asList(fieldsToTranslateStr.split(CSV_DELIMITER)));
        }
    }

    // Load shared translation groups from a comma-separated string
    private static void loadSharedTranslationGroups(String sharedTranslationGroupsStr) {
        if (sharedTranslationGroupsStr != null && !sharedTranslationGroupsStr.isEmpty()) {
            Arrays.stream(sharedTranslationGroupsStr.split(CSV_DELIMITER))
                  .map(group -> group.split(":"))
                  .filter(fields -> fields.length == 2)
                  .forEach(fields -> sharedTranslationGroups.put(fields[0], fields[1]));
        }
    }

    // Load translation map from a CSV file
    private static void loadTranslationMap(String translationFilePath) {
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(translationFilePath))) {
            reader.lines()
                  .map(line -> line.split(CSV_DELIMITER))
                  .filter(mapping -> mapping.length == 3)
                  .forEach(mapping -> translationMap
                      .computeIfAbsent(mapping[0], k -> new HashMap<>())
                      .put(mapping[1], mapping[2]));
        } catch (IOException e) {
            logError("Error loading translation file: " + e.getMessage());
        }
    }

    // Process the input file and write the translated output to the output file
    private static void processFile(String inputFilePath, String outputFilePath) {
        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        List<Future<String>> futures = new ArrayList<>();

        try (BufferedReader reader = Files.newBufferedReader(Paths.get(inputFilePath));
             PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(outputFilePath)))) {

            // Read and write the header line
            String headerLine = reader.readLine();
            if (headerLine == null) return;
            String[] headers = headerLine.split(DELIMITER);
            writer.println(headerLine);

            // Process each line in parallel
            reader.lines().forEach(line -> {
                futures.add(executor.submit(() -> {
                    String[] fields = line.split(DELIMITER);
                    return translateLine(headers, fields);
                }));
            });

            // Write the translated lines to the output file
            for (Future<String> future : futures) {
                try {
                    writer.println(future.get());
                } catch (InterruptedException | ExecutionException e) {
                    logError("Error processing line: " + e.getMessage());
                }
            }

        } catch (IOException e) {
            logError("Error reading input file: " + e.getMessage());
        } finally {
            executor.shutdown();
        }
    }

    // Translate a line of fields based on the headers
    private static String translateLine(String[] headers, String[] fields) {
        for (int i = 0; i < fields.length; i++) {
            if (fieldsToTranslate.contains(headers[i])) {
                fields[i] = translateValue(headers[i], fields[i]);
            }
        }
        return String.join("|", fields);
    }

    // Translate a value based on the field name and translation map
    private static String translateValue(String fieldName, String value) {
        String group = sharedTranslationGroups.getOrDefault(fieldName, fieldName);
        return translationMap.getOrDefault(group, Collections.emptyMap()).getOrDefault(value, value);
    }

    // Log an error message to the console and error log file
    private static void logError(String message) {
        System.err.println(message);
        try (PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(ERROR_LOG_FILE, true)))) {
            out.println(message);
        } catch (IOException e) {
            System.err.println("Error writing to log file: " + e.getMessage());
        }
    }
}