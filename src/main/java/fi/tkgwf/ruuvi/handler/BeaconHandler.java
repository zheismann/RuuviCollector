package fi.tkgwf.ruuvi.handler;

import fi.tkgwf.ruuvi.Main;
import fi.tkgwf.ruuvi.bean.EnhancedRuuviMeasurement;
import fi.tkgwf.ruuvi.bean.HCIData;
import fi.tkgwf.ruuvi.common.bean.RuuviMeasurement;
import fi.tkgwf.ruuvi.common.parser.DataFormatParser;
import fi.tkgwf.ruuvi.common.parser.impl.AnyDataFormatParser;
import fi.tkgwf.ruuvi.config.Config;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
/**
 * Creates {@link RuuviMeasurement} instances from raw dumps from hcidump.
 */
public class BeaconHandler {

    private static final Logger LOG = LogManager.getLogger(BeaconHandler.class);
    private final DataFormatParser parser = new AnyDataFormatParser();
    private final Set<String> macAddressesFound = new HashSet<>();

    /**
     * Handles a packet and creates a {@link RuuviMeasurement} if the handler
     * understands this packet.
     *
     * @param hciData the data parsed from hcidump
     * @return an instance of a {@link EnhancedRuuviMeasurement} if this handler can
     * parse the packet
     */
    public Optional<EnhancedRuuviMeasurement> handle(HCIData hciData) {
        HCIData.Report.AdvertisementData adData = hciData.findAdvertisementDataByType(0xFF); // Manufacturer-specific data, raw dataformats
        if (adData == null) {
            adData = hciData.findAdvertisementDataByType(0x16); // Eddystone url
            if (adData == null) {
                return Optional.empty();
            }
        }
        RuuviMeasurement measurement = parser.parse(adData.dataBytes());
        if (measurement == null) {
            return Optional.empty();
        }
        if (!macAddressesFound.contains(hciData.mac)) {
            macAddressesFound.add(hciData.mac);
            LOG.debug("Saw MAC for the first time: " + hciData.mac + "; rssi: " + hciData.rssi + "; battery voltage: " + measurement.getBatteryVoltage() + "; txPower: " + measurement.getTxPower() );
        }
        EnhancedRuuviMeasurement enhancedMeasurement = new EnhancedRuuviMeasurement(measurement);
        enhancedMeasurement.setMac(hciData.mac);
        enhancedMeasurement.setRssi(hciData.rssi);
        enhancedMeasurement.setTime( Instant.now().toEpochMilli() );
        enhancedMeasurement.setName(Config.getTagName(hciData.mac));
        return Optional.of(enhancedMeasurement);
    }
}
