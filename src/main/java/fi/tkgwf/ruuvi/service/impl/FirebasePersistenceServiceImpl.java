package fi.tkgwf.ruuvi.service.impl;

import com.google.api.core.ApiFuture;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.WriteBatch;
import com.google.cloud.firestore.WriteResult;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.cloud.FirestoreClient;

import fi.tkgwf.ruuvi.bean.EnhancedRuuviMeasurement;
import fi.tkgwf.ruuvi.config.FirebaseConfig;
import fi.tkgwf.ruuvi.service.PersistenceService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
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
    private static final Logger LOG = LogManager.getLogger( FirebasePersistenceServiceImpl.class );

    private Firestore db;
    private CollectionReference measurementHistoryCollection;
    private CollectionReference recentSensorReadingsCollection;

    private final ArrayBlockingQueue<EnhancedRuuviMeasurement> arrayBlockingQueue =
        new ArrayBlockingQueue<>( 250000, true );
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool( 1 );

    public FirebasePersistenceServiceImpl()
    {
        this( FirebaseConfig.getProjectId(), FirebaseConfig.getServiceAccountJSONPrivateKey() );
    }

    protected FirebasePersistenceServiceImpl( String projectId, Path serviceAccountJSONPrivateKey )
    {
        try ( InputStream serviceAccount = new FileInputStream( serviceAccountJSONPrivateKey.toFile() ) )
        {
            GoogleCredentials credentials = GoogleCredentials.fromStream( serviceAccount );
            FirebaseOptions options = FirebaseOptions.builder()
                .setCredentials( credentials )
                .setProjectId( projectId )
                .build();
            FirebaseApp.initializeApp( options );

            db = FirestoreClient.getFirestore();
            measurementHistoryCollection = db.collection( FirebaseConfig.getMeasurementHistoryCollectionName() );
            recentSensorReadingsCollection = db.collection( FirebaseConfig.getRecentSensorReadingsCollectionName() );
            scheduler.scheduleWithFixedDelay( new FirebaseWriter(), 1, 1, TimeUnit.MINUTES );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Failure creating connection to Firebase projectId: " + projectId + " using key: " + serviceAccountJSONPrivateKey, e );
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
                    final DocumentReference recentSensorReadingDocRef = recentSensorReadingsCollection.document( measurement.getMac() );
                    Map<String, Object> sensorData = new HashMap<>();
                    sensorData.put( "lastSensorReadingTimestamp", new java.util.Date() );
                    sensorData.put( "lastTemperatureReading", measurement.getTemperature() );

                    final ApiFuture<DocumentSnapshot> documentSnapshotApiFuture = recentSensorReadingDocRef.get();
                    final DocumentSnapshot documentSnapshot = documentSnapshotApiFuture.get();
                    final LocalDate today = LocalDate.now();
                    if ( !documentSnapshot.exists() )
                    {
                        final HashMap<String, Map<String,Object>> dailyMinMaxRecords = new HashMap<>();
                        sensorData.put( "dailyMinMaxRecords", dailyMinMaxRecords );
                        final Map<String,Object> minMaxData = new HashMap<>();
                        minMaxData.put( "maxTemperatureReading", measurement.getTemperature() );
                        minMaxData.put( "maxTemperatureTimestamp", new java.util.Date() );
                        minMaxData.put( "minTemperatureReading", measurement.getTemperature() );
                        minMaxData.put( "minTemperatureTimestamp", new java.util.Date() );
                        dailyMinMaxRecords.put( today.format( DateTimeFormatter.BASIC_ISO_DATE ), minMaxData );
                        batch.create( recentSensorReadingDocRef, sensorData );
                    }
                    else
                    {
                        final Map<String, Map<String,Object>> dailyMinMaxRecords = (Map<String, Map<String,Object>>)documentSnapshot.get( "dailyMinMaxRecords" );
                        removeOlderMinMaxRecordMaps( today, dailyMinMaxRecords );
                        final String todayBasicISODateStr = today.format(DateTimeFormatter.BASIC_ISO_DATE);
                        if ( dailyMinMaxRecords.containsKey( todayBasicISODateStr ) )
                        {
                            sensorData.put("dailyMinMaxRecords", dailyMinMaxRecords );
                            final Map<String, Object> minMaxData = dailyMinMaxRecords.get( todayBasicISODateStr );
                            final double currentMaxTemperature = ( Double ) minMaxData.get( "maxTemperatureReading" );
                            if ( measurement.getTemperature() >= currentMaxTemperature )
                            {
                                minMaxData.put( "maxTemperatureReading", measurement.getTemperature() );
                                minMaxData.put( "maxTemperatureTimestamp", new java.util.Date() );
                            }
                            final double currentMinTemperature = ( Double ) minMaxData.get( "minTemperatureReading" );
                            if ( measurement.getTemperature() <= currentMinTemperature )
                            {
                                minMaxData.put( "minTemperatureReading", measurement.getTemperature() );
                                minMaxData.put( "minTemperatureTimestamp", new java.util.Date() );
                            }
                        }
                        else
                        {
                            HashMap<String, Object> minMaxData = new HashMap<>();
                            dailyMinMaxRecords.put( todayBasicISODateStr, minMaxData );
                            sensorData.put("dailyMinMaxRecords", dailyMinMaxRecords );
                            minMaxData.put( "maxTemperatureReading", measurement.getTemperature() );
                            minMaxData.put( "maxTemperatureTimestamp", new java.util.Date() );
                            minMaxData.put( "minTemperatureReading", measurement.getTemperature() );
                            minMaxData.put( "minTemperatureTimestamp", new java.util.Date() );
                        }
                        LOG.info( "batch.update( recentSensorReadingDocRef, "+sensorData+" ) " );
                        batch.update( recentSensorReadingDocRef, sensorData );
                    }

                    final DocumentReference ruuviMeasurementDocument = measurementHistoryCollection.document();
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

        private void removeOlderMinMaxRecordMaps( LocalDate today, Map<String, Map<String, Object>> dailyMinMaxRecords )
        {
            LocalDate previousDay = today.minus( 5, ChronoUnit.DAYS );
            String previousISODateStr = previousDay.format( DateTimeFormatter.BASIC_ISO_DATE );
            while ( dailyMinMaxRecords.containsKey( previousISODateStr ) )
            {
                dailyMinMaxRecords.remove( previousISODateStr );
                LOG.debug( "Removed min max record map for " + previousISODateStr );
                previousDay = previousDay.minus( 1, ChronoUnit.DAYS );
                previousISODateStr = previousDay.format( DateTimeFormatter.BASIC_ISO_DATE );
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
                    completedBatchFutures.add( future );
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
