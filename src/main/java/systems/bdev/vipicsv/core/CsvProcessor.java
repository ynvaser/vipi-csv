package systems.bdev.vipicsv.core;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;

@Slf4j
public class CsvProcessor {
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("uuuu.MM.d H:mm");
    protected static final CSVFormat CSV_FORMAT = CSVFormat.EXCEL.withDelimiter(';');
    private static final Character UTF8_BOM = '\uFEFF';
    private final long interval;
    protected final List<CsvRecord> csvRecords;

    public CsvProcessor(long interval, File file) {
        this.interval = interval;
        csvRecords = process(file);
    }

    public String getProcessedFileContents() {
        Map<Long, Map<String, List<CsvRecord>>> resultHolder = createSortedResultHolder();
        populateResultColumn(resultHolder);
        return createCsvContents(csvRecords);
    }

    protected Map<Long, Map<String, List<CsvRecord>>> createSortedResultHolder() {
        Map<Long, Map<String, List<CsvRecord>>> resultHolder = new HashMap<>();
        for (CsvRecord record : csvRecords) {
            Long cameraNumber = record.getCameraNumber();
            String speciesName = record.getSpeciesName().toUpperCase();
            if (!resultHolder.containsKey(cameraNumber)) {
                resultHolder.put(cameraNumber, new HashMap<>());
            }
            Map<String, List<CsvRecord>> speciesMap = resultHolder.get(cameraNumber);
            if (!speciesMap.containsKey(speciesName)) {
                speciesMap.put(speciesName, new ArrayList<>());
            }
            List<CsvRecord> recordingInstances = speciesMap.get(speciesName);
            recordingInstances.add(record);
        }
        resultHolder.values().forEach(speciesMap -> speciesMap.values().forEach(Collections::sort));
        return resultHolder;
    }

    protected void populateResultColumn(Map<Long, Map<String, List<CsvRecord>>> resultHolder) {
        resultHolder
                .values()
                .forEach(speciesMap -> speciesMap
                        .values()
                        .forEach(records -> {
                            LocalDateTime actual = null;
                            for (CsvRecord record : records) {
                                if (actual == null || ChronoUnit.MINUTES.between(actual, record.getDateTime()) >= interval) {
                                    record.setResultColumn(record.getSpeciesName());
                                    actual = record.getDateTime();
                                }
                            }
                        }));
    }

    private String createCsvContents(List<CsvRecord> csvRecords) {
        StringBuffer stringBuffer = new StringBuffer();
        try (CSVPrinter printer = new CSVPrinter(stringBuffer, CSV_FORMAT)) {
            for (CsvRecord csvRecord : csvRecords) {
                printer.printRecord(csvRecord.getCameraNumber(), DATE_TIME_FORMATTER.format(csvRecord.getDateTime()), csvRecord.getResultColumn());
            }
        } catch (IOException e) {
            log.error("Can't create CSV contents!", e);
            throw new RuntimeException(e);
        }
        return stringBuffer.toString();
    }

    private List<CsvRecord> process(final File file) {
        List<CsvRecord> result = new ArrayList<>();
        try (InputStreamReader fileReader = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8.newDecoder())) {
            CSVParser parsedCsv = CSV_FORMAT
                    .parse(fileReader);
            for (CSVRecord record : parsedCsv) {
                if (record.size() != 3) {
                    log.error("File {} line {} isn't of length 3!", file.getName(), record.getRecordNumber());
                    throw new RuntimeException("File " + file.getName() + " line " + record.getRecordNumber() + " isn't of length 3!");
                } else if ("Site_ID".equalsIgnoreCase(record.get(0))) {
                    log.info("Skipping header.");
                } else {
                    CsvRecord csvRecord = new CsvRecord();
                    validateRecord(record);
                    csvRecord.setCameraNumber(Long.parseLong(removeBOMIfPresent(record.get(0))));
                    csvRecord.setDateTime(LocalDateTime.from(DATE_TIME_FORMATTER.parse(record.get(1))));
                    csvRecord.setSpeciesName(record.get(2));
                    result.add(csvRecord);
                }
            }
        } catch (IOException e) {
            log.error("Couldn't open file: {}", file.getName(), e);
            throw new RuntimeException(e);
        }
        return result;
    }

    private void validateRecord(CSVRecord record) {
        if (record.get(0).isBlank()) {
            throw new IllegalArgumentException("No camera number on line: " + record.getRecordNumber());
        } else if (record.get(1).isBlank()) {
            throw new IllegalArgumentException("No date and time on line: " + record.getRecordNumber());
        } else if (record.get(2).isBlank()) {
            throw new IllegalArgumentException("No species on line: " + record.getRecordNumber());
        }
    }

    private String removeBOMIfPresent(String s) {
        String prunedString = s;
        if (UTF8_BOM.equals(s.charAt(0))) {
            prunedString = s.substring(1);
        }
        return prunedString.replaceAll("[^\\d.]", "");
    }
}
