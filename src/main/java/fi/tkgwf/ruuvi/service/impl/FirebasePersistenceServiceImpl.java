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
import fi.tkgwf.ruuvi.config.FirebaseConfig;
import fi.tkgwf.ruuvi.service.PersistenceService;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class FirebasePersistenceServiceImpl implements PersistenceService
{
    private static final Logger LOG = Logger.getLogger( FirebasePersistenceServiceImpl.class );

    private Firestore db;
    private CollectionReference firebaseMeasurementHistoryCollection;
    private CollectionReference firebaseMostRecentMeasurementCollection;

    private final ArrayBlockingQueue<EnhancedRuuviMeasurement> arrayBlockingQueue =
        new ArrayBlockingQueue<>( 250000, true );
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool( 1 );

    public FirebasePersistenceServiceImpl()
    {
        this( FirebaseConfig.getFirebaseProjectId(), FirebaseConfig.getFirebaseServiceAccountJSONPrivateKey(), FirebaseConfig.getFirebaseMeasurementHistoryCollectionName(), FirebaseConfig.getFirebaseMostRecentMeasurementCollectionName() );
    }

    protected FirebasePersistenceServiceImpl( String firebaseProjectId, Path serviceAccountJSONPrivateKey, String firebaseMeasurementHistoryCollectionName, String firebaseMostRecentMeasurementCollectionName )
    {
        try ( InputStream serviceAccount = new FileInputStream( serviceAccountJSONPrivateKey.toFile() ) )
        {
            GoogleCredentials credentials = GoogleCredentials.fromStream( serviceAccount );
            FirebaseOptions options = FirebaseOptions.builder()
                .setCredentials( credentials )
                .setProjectId( firebaseProjectId )
                .build();
            FirebaseApp.initializeApp( options );

            db = FirestoreClient.getFirestore();
            firebaseMeasurementHistoryCollection = db.collection( firebaseMeasurementHistoryCollectionName );
            firebaseMostRecentMeasurementCollection = db.collection( firebaseMostRecentMeasurementCollectionName );
            scheduler.scheduleWithFixedDelay( new FirebaseWriter(), 1, 1, TimeUnit.MINUTES );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Failure creating connection to Firebase projectId: " + firebaseProjectId + " using key: " + serviceAccountJSONPrivateKey, e );
        }
    }

    @Override
    public void close()
    {
        try
        {
            db.close();
        }
        catch ( Exception exc )
        {
            LOG.warn( "Failure occurred while closing connection to Firestore database" );
        }
    }

    @Override
    public void store( final EnhancedRuuviMeasurement measurement )
    {
/*
        TODO: use limitingStrategy?  or does that only make sense for InfluxDB?
        Optional.ofNullable(measurement.getMac())
            .map( Config::getLimitingStrategy)
            .orElse(limitingStrategy)
            .apply(measurement)
            .ifPresent(db::save);
*/
        boolean measurementAdded = arrayBlockingQueue.offer( measurement );
        if ( !measurementAdded )
        {
            LOG.error( "Failed to add measurement: " + measurement );
        }

    }

    private class FirebaseWriter implements Runnable
    {
        final Map<String, EnhancedRuuviMeasurement> recordedMeasurementsMap = new HashMap<>();
        private final List<ApiFuture<List<WriteResult>>> batchFutures = new ArrayList<>();
        private final List<ApiFuture<WriteResult>> futures = new ArrayList<>();

        @Override
        public void run()
        {
            waitForPreviousWriteEventsToFinish();

            try
            {
                // batches and transactions: https://firebase.google.com/docs/firestore/manage-data/transactions
                WriteBatch batch = db.batch();

                final int MAXIMUM_MEASUREMENTS_TO_WRITE = 500; // per the API
                List<EnhancedRuuviMeasurement> measurementsRecorded = new ArrayList<>();
                arrayBlockingQueue.drainTo( measurementsRecorded, MAXIMUM_MEASUREMENTS_TO_WRITE );
                LOG.info("measurementsRecorded.size(): " + measurementsRecorded.size() );

                Set<String> macAddresses = new HashSet<>();
                for ( EnhancedRuuviMeasurement measurement : measurementsRecorded )
                {
                    if ( !shouldBeRecorded( measurement ) )
                    {
                        continue;
                    }
                    final DocumentReference mostRecentMeasurementDocument = firebaseMostRecentMeasurementCollection.document( measurement.getMac() );
                    Map<String, Object> mostRecentMeasurementData = new HashMap<>();
                    mostRecentMeasurementData.put( "time", new java.util.Date() );
                    mostRecentMeasurementData.put( "temperature", measurement.getTemperature() );
                    ApiFuture<WriteResult> future = mostRecentMeasurementDocument.update( mostRecentMeasurementData );
                    futures.add( future );

                    final DocumentReference ruuviMeasurementDocument = firebaseMeasurementHistoryCollection.document();
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

                    macAddresses.add( measurement.getMac() );

                    // TODO: store these measurements in a queue that will be read by another thread that will
                    //  create actual batches
                    batch.create( ruuviMeasurementDocument, data );
                    recordedMeasurementsMap.put( measurement.getMac(), measurement );
                }
                LOG.info("finished creating ruuviMeasurementDocument for these mac addresses: " + macAddresses );
                measurementsRecorded.clear();
                LOG.info("Number of measurements to write via batch: " + batch.getMutationsSize() );

                // asynchronously commit the batch
                ApiFuture<List<WriteResult>> batchFutures = batch.commit();
                this.batchFutures.add( batchFutures );
            }
            catch ( Throwable t )
            {
                LOG.error( "Encountered error in FirebaseWriter. " + t.getMessage(), t );
            }
        }

        private void waitForPreviousWriteEventsToFinish()
        {
            LOG.info( "FirebaseWriter.run()  batchFutures " + batchFutures.size() + "\tarrayBlockingQueue " + arrayBlockingQueue.size() );
            List<ApiFuture<List<WriteResult>>> completedBatchFutures = new ArrayList<>();
            for ( ApiFuture<List<WriteResult>> future : batchFutures )
            {
                try
                {
                    future.get();
                    completedBatchFutures.add( future );
                }
                catch ( Throwable t )
                {
                    LOG.error( "Encountered error while waiting on an ApiFuture to complete. " + t.getMessage(), t );
                }
            }
            batchFutures.removeAll( completedBatchFutures );
            completedBatchFutures.clear();
            if ( !batchFutures.isEmpty() )
            {
                LOG.info( "batchFutures is not empty! futures.size(): " + batchFutures.size() );
            }

            LOG.info( "FirebaseWriter.run()  futures " + futures.size() );
            List<ApiFuture<WriteResult>> completedFutures = new ArrayList<>();
            for ( ApiFuture<WriteResult> future : futures )
            {
                try
                {
                    future.get();
                    completedFutures.add( future );
                }
                catch ( Throwable t )
                {
                    LOG.error( "Encountered error while waiting on an ApiFuture to complete. " + t.getMessage(), t );
                }
            }
            futures.removeAll( completedFutures );
            completedFutures.clear();
            if ( !futures.isEmpty() )
            {
                LOG.info( "futures is not empty! futures.size(): " + futures.size() );
            }
        }

        private boolean shouldBeRecorded( EnhancedRuuviMeasurement t )
        {
            boolean shouldBeRecorded = true;
            String currentMAC = t.getMac();
            Long currentMeasurementTime = t.getTime();
            if ( recordedMeasurementsMap.containsKey( currentMAC ) )
            {
                Long previousMeasurementTime = recordedMeasurementsMap.get( currentMAC ).getTime();
                final int ONE_MINUTE_IN_MILLISECONDS = 1000 * 60;
                if ( ( currentMeasurementTime - previousMeasurementTime ) < ONE_MINUTE_IN_MILLISECONDS )
                {
                    shouldBeRecorded = false;
                }
            }
            return shouldBeRecorded;
        }
    }
}
