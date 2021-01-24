package fi.tkgwf.ruuvi.service.impl;

import com.google.api.core.ApiFuture;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.WriteBatch;
import com.google.cloud.firestore.WriteResult;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.cloud.FirestoreClient;

import fi.tkgwf.ruuvi.bean.EnhancedRuuviMeasurement;
import fi.tkgwf.ruuvi.config.Config;
import fi.tkgwf.ruuvi.config.FirebaseConfig;
import fi.tkgwf.ruuvi.service.PersistenceService;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class FirebasePersistenceServiceImpl implements PersistenceService {
    private static final Logger LOG = Logger.getLogger( FirebasePersistenceServiceImpl.class);

    private Firestore db;
    private CollectionReference collection;

    public FirebasePersistenceServiceImpl() {
        this(FirebaseConfig.getFirebaseProjectId(), FirebaseConfig.getFirebaseServiceAccountJSONPrivateKey(), FirebaseConfig.getFirebaseCollectionName());
    }

    protected FirebasePersistenceServiceImpl( String firebaseProjectId, Path serviceAccountJSONPrivateKey, String firebaseCollectionName ) {
        try ( InputStream serviceAccount = new FileInputStream(serviceAccountJSONPrivateKey.toFile() ) ) {
            GoogleCredentials credentials = GoogleCredentials.fromStream( serviceAccount );
            FirebaseOptions options = FirebaseOptions.builder()
                .setCredentials( credentials )
                .setProjectId( firebaseProjectId )
                .build();
            FirebaseApp.initializeApp( options );

            db = FirestoreClient.getFirestore();
            collection = db.collection( firebaseCollectionName );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Failure creating connection to Firebase projectId: " + firebaseProjectId + " using key: " + serviceAccountJSONPrivateKey, e );
        }
    }

    @Override
    public void close() {
        try {
            db.close();
        }
        catch ( Exception exc ) {
            LOG.warn( "Failure occurred while closing connection to Firestore database" );
        }
    }

    @Override
    public void store(final EnhancedRuuviMeasurement measurement) {
/*
        TODO: use limitingStrategy?  or does that only make sense for InfluxDB?
        Optional.ofNullable(measurement.getMac())
            .map( Config::getLimitingStrategy)
            .orElse(limitingStrategy)
            .apply(measurement)
            .ifPresent(db::save);
*/

        // batches and transactions: https://firebase.google.com/docs/firestore/manage-data/transactions
        WriteBatch batch = db.batch();

        final DocumentReference ruuviMeasurementDocument = collection.document();

        // TODO: only record properties as defined in Config.getStorageValueSet()
        Map<String, Object> data = new HashMap<>();
        data.put( "mac", measurement.getMac() );
        data.put( "time", measurement.getTime() );
        data.put( "rssi", measurement.getRssi() );
        data.put( "temperature", measurement.getTemperature() );
        data.put( "txPower", measurement.getTxPower() );
        data.put( "batteryVoltage", measurement.getBatteryVoltage() );
        data.put( "pressure", measurement.getPressure() );
        data.put( "humidity", measurement.getHumidity() );


        // TODO: store these measurements in a queue that will be read by another thread that will
        //  create actual batches
        batch.create( ruuviMeasurementDocument, data );

        // asynchronously commit the batch
        ApiFuture<List<WriteResult>> future = batch.commit();

        // future.get() blocks on batch commit operation
        try
        {
            for (WriteResult result : future.get()) {
                System.out.println( "Update time : " + result.getUpdateTime() );
            }
        }
        catch ( Exception e ) {
            LOG.error( "Failed writing to Firebase message: " + e.getMessage(), e );
        }
    }

}
