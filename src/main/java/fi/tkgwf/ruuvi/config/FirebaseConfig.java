package fi.tkgwf.ruuvi.config;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;

public abstract class FirebaseConfig {

    private static final Logger LOG = Logger.getLogger(FirebaseConfig.class);
    private static final String FIREBASE_PROPERTIES = "ruuvi-firebase.properties";

    private static String firebaseProjectId;
    private static String firebaseMeasurementHistoryCollectionName;
    private static String firebaseMostRecentMeasurementCollectionName;
    private static Path firebaseServiceAccountJSONPrivateKey;

    private static Function<String, File> configFileFinder;


    static {
        reload();
    }

    public static void reload() {
        reload(defaultConfigFileFinder());
    }

    public static void reload(final Function<String, File> configFileFinder) {
        FirebaseConfig.configFileFinder = configFileFinder;
        readConfig();
    }


    private static void readConfig() {
        try {
            final File configFile = configFileFinder.apply( FIREBASE_PROPERTIES );
            if (configFile != null) {
                LOG.debug("FirebaseConfig: " + configFile);
                Properties props = new Properties();
                props.load(new InputStreamReader(new FileInputStream(configFile), Charset.forName("UTF-8")));
                LOG.debug("props: " + props);
                readConfigFromProperties(props);
            }
        } catch ( IOException ex) {
            LOG.warn("Failed to read configuration, using default values...", ex);
        }
    }

    public static void readConfigFromProperties(final Properties props) {
        firebaseProjectId = props.getProperty("firebaseProjectId");
        LOG.debug( "firebaseProjectId = " + firebaseProjectId );
        firebaseMeasurementHistoryCollectionName = props.getProperty("firebaseMeasurementHistoryCollectionName");
        LOG.debug( "firebaseMeasurementHistoryCollectionName = " + firebaseMeasurementHistoryCollectionName );
        firebaseMostRecentMeasurementCollectionName = props.getProperty("firebaseMostRecentMeasurementCollectionName");
        LOG.debug( "firebaseMostRecentMeasurementCollectionName = " + firebaseMostRecentMeasurementCollectionName );
        LOG.debug( "props.getProperty(\"firebaseServiceAccountJSONPrivateKey\") = " + props.getProperty("firebaseServiceAccountJSONPrivateKey") );
        final File privateKeyFile = configFileFinder.apply( props.getProperty("firebaseServiceAccountJSONPrivateKey") );
        LOG.debug( "privateKeyFile = " + privateKeyFile );
        firebaseServiceAccountJSONPrivateKey = privateKeyFile == null ? null : privateKeyFile.toPath();
        LOG.debug( "firebaseServiceAccountJSONPrivateKey = " + firebaseServiceAccountJSONPrivateKey );
        validateConfig();
    }

    private static Path findFirebasePrivateKeyFile( String privateKeyLocation ) {
        return null;
    }

    private static void validateConfig() {
        if (firebaseProjectId == null || firebaseProjectId.trim().isEmpty()) {
            throw new IllegalStateException("The firebaseProjectId property must be specified in the FIREBASE_PROPERTIES file.");
        }
        else if ( firebaseMeasurementHistoryCollectionName == null || firebaseMeasurementHistoryCollectionName.trim().isEmpty()) {
            throw new IllegalStateException("The firebaseMeasurementHistoryCollectionName property must be specified in the FIREBASE_PROPERTIES file.");
        }
        else if ( firebaseMostRecentMeasurementCollectionName == null || firebaseMostRecentMeasurementCollectionName.trim().isEmpty()) {
            throw new IllegalStateException("The firebaseMostRecentMeasurementCollectionName property must be specified in the FIREBASE_PROPERTIES file.");
        }
        else if (firebaseServiceAccountJSONPrivateKey == null) {
            throw new IllegalStateException("The firebaseServiceAccountJSONPrivateKey property must be specified in the FIREBASE_PROPERTIES file.");
        }
        else if (!Files.exists(firebaseServiceAccountJSONPrivateKey)) {
            throw new IllegalStateException("The firebaseServiceAccountJSONPrivateKey property's value does not correspond to an actual file.  Value: " + firebaseServiceAccountJSONPrivateKey );
        }
        else if (!Files.isReadable(firebaseServiceAccountJSONPrivateKey)) {
            throw new IllegalStateException("The firebaseServiceAccountJSONPrivateKey property's value does not correspond to a file that is readable by the process.  Value: " + firebaseServiceAccountJSONPrivateKey );
        }
        else if (Files.isDirectory(firebaseServiceAccountJSONPrivateKey)) {
            throw new IllegalStateException("The firebaseServiceAccountJSONPrivateKey property's value corresponds to a directory, it should be a file.  Value: " + firebaseServiceAccountJSONPrivateKey );
        }
    }

    private static Function<String, File> defaultConfigFileFinder() {
        return propertiesFileName -> {
            try {
                final File jarLocation = new File(FirebaseConfig.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath()).getParentFile();
                Optional<File> configFile = findConfigFile(propertiesFileName, jarLocation);
                if (!configFile.isPresent()) {
                    // look for config files in the parent directory if none found in the current directory, this is useful during development when
                    // RuuviCollector can be run from maven target directory directly while the config file sits in the project root
                    final File parentFile = jarLocation.getParentFile();
                    configFile = findConfigFile(propertiesFileName, parentFile);
                }
                return configFile.orElse(null);
            } catch ( URISyntaxException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private static Optional<File> findConfigFile(String propertiesFileName, File parentFile) {
        return Optional.ofNullable(parentFile.listFiles(f -> f.isFile() && f.getName().equals(propertiesFileName)))
            .filter(configFiles -> configFiles.length > 0)
            .map(configFiles -> configFiles[0]);
    }


    public static String getFirebaseProjectId() {
        return firebaseProjectId;
    }

    public static Path getFirebaseServiceAccountJSONPrivateKey() {
        return firebaseServiceAccountJSONPrivateKey;
    }

    public static String getFirebaseMeasurementHistoryCollectionName() {
        return firebaseMeasurementHistoryCollectionName;
    }

    public static String getFirebaseMostRecentMeasurementCollectionName()
    {
        return firebaseMostRecentMeasurementCollectionName;
    }
}
