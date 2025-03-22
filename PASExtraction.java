public class PASExtraction {
    // Default config file path
    private static final String DEFAULT_CONFIG_FILE_PATH = "config.properties";

    public static void main(String[] args) {
        String operation = "translate"; // Default operation
        String configFilePath = DEFAULT_CONFIG_FILE_PATH; // Default config file path

        if (args.length > 0) {
            operation = args[0];
        }

        if (args.length > 1) {
            configFilePath = args[1];
        } else if (args.length == 1) {
            System.out.println("Using default config file path: " + configFilePath);
        } else {
            System.out.println("Usage: java Main <operation> [configFilePath]");
            System.out.println("Using default operation: " + operation);
            System.out.println("Using default config file path: " + configFilePath);
        }

        // Execute the specified operation
        switch (operation.toLowerCase()) {
            case "export":
                FetchDataToFile.getData(configFilePath);
                break;
            case "translate":
                ValueTranslation.translateValues(configFilePath);
                break;
            case "fetch_n_translate":
                String inputFilePath = FetchDataToFile.getData(configFilePath);
                if (inputFilePath != null) {
                    ValueTranslation.translateValues(configFilePath, inputFilePath);
                }
                break;
            default:
                System.out.println("Invalid operation. Use 'export', 'translate', or 'fetch_n_translate'.");
                break;
        }
    }
}
