package systems.bdev.vipicsv.core;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class MatrixCsvProcessor extends CsvProcessor {
    private static final String IN_PROGRESS = "folyamatban";
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("uuuu.MM.dd");

    private final Map<Tuple<Long, YearMonth>, Map<String, Integer>> resultMap = new TreeMap<>();
    private final Set<String> speciesInOrder = new TreeSet<>();
    private final Map<Long, Tuple<YearMonth, YearMonth>> cameraActivityPeriods;

    public MatrixCsvProcessor(long interval, File file, File cameraActivityFile) {
        super(interval, file);
        cameraActivityPeriods = parseCameraActivityFile(cameraActivityFile);
    }

    private Map<Long, Tuple<YearMonth, YearMonth>> parseCameraActivityFile(File cameraActivityFile) {
        Map<Long, Tuple<YearMonth, YearMonth>> result = new HashMap<>();
        try (InputStreamReader fileReader = new InputStreamReader(new FileInputStream(cameraActivityFile), StandardCharsets.UTF_8.newDecoder())) {
            CSVParser parsedCsv = CSV_FORMAT
                    .parse(fileReader);
            long i = 0;
            for (CSVRecord record : parsedCsv) {
                if (record.size() != 2 && record.size() != 3) {
                    log.error("Camera activity file {} line {} isn't of length 2 or 3!", cameraActivityFile.getName(), record.getRecordNumber());
                    throw new RuntimeException("Camera activity file " + cameraActivityFile.getName() + " line " + record.getRecordNumber() + " isn't of length 2!");
                } else if (record.get(0).contains("kihely") || record.get(1).contains("kihely")) {
                    log.info("Skipping header");
                } else {
                    long siteId = ++i;
                    boolean isSiteIdSpecified = record.size() == 3;
                    if (isSiteIdSpecified) {
                        siteId = Integer.parseInt(record.get(0));
                    }
                    String to = record.get(isSiteIdSpecified ? 2 : 1).strip();
                    if (IN_PROGRESS.equalsIgnoreCase(to)) {
                        to = "2077.01.01";
                    }
                    result.put(siteId, new Tuple<>(YearMonth.from(DATE_FORMATTER.parse(record.get(isSiteIdSpecified ? 1 : 0))), YearMonth.from(DATE_FORMATTER.parse(to))));

                }
            }
        } catch (IOException e) {
            log.error("Couldn't open file: {}", cameraActivityFile.getName(), e);
            throw new RuntimeException(e);
        }
        return result;
    }

    @Override
    public String getProcessedFileContents() {
        populateResultColumn(createSortedResultHolder());
        List<CsvRecord> duplicateDetections = csvRecords.stream().filter(csvRecord -> csvRecord.getResultColumn() != null && !csvRecord.getResultColumn().isBlank()).collect(Collectors.toList());
        csvRecords.removeAll(duplicateDetections);
        populateMatrixWithNA();
        populateMatrixWithDetections();
        return createMatrixCsvContents();
    }

    private String createMatrixCsvContents() {
        StringBuffer stringBuffer = new StringBuffer();
        try (CSVPrinter printer = new CSVPrinter(stringBuffer, CSV_FORMAT)) {
            printHeader(printer);
            for (Map.Entry<Tuple<Long, YearMonth>, Map<String, Integer>> entry : resultMap.entrySet()) {
                Tuple<Long, YearMonth> cameraYearMonth = entry.getKey();
                Map<String, Integer> speciesObservationCount = entry.getValue();
                printer.print(cameraYearMonth.getKey());
                printer.print(cameraYearMonth.getValue().getYear());
                printer.print(cameraYearMonth.getValue().getMonthValue());
                for (String species : speciesInOrder) {
                    Integer observationCount = speciesObservationCount.getOrDefault(species, -2);
                    String observationCountText = observationCount >= 0 ? observationCount.toString() : observationCount == -1 ? "N/A" : "ERROR";
                    printer.print(observationCountText);
                }
                printer.println();
            }
        } catch (IOException e) {
            log.error("Can't create CSV contents!", e);
            throw new RuntimeException(e);
        }
        return stringBuffer.toString();
    }

    private void printHeader(CSVPrinter printer) throws IOException {
        printer.print("cameraNumber");
        printer.print("year");
        printer.print("month");
        for (String s : speciesInOrder) {
            printer.print(s);
        }
        printer.println();
    }

    private void populateMatrixWithDetections() {
        for (CsvRecord csvRecord : csvRecords) {
            Long cameraNumber = csvRecord.getCameraNumber();
            LocalDateTime dateTime = csvRecord.getDateTime();
            YearMonth yearMonth = YearMonth.of(dateTime.getYear(), dateTime.getMonth());
            Tuple<Long, YearMonth> key = new Tuple<>(cameraNumber, yearMonth);
            String speciesName = csvRecord.getSpeciesName().toUpperCase();
            Map<String, Integer> cameraMonth = resultMap.get(key);
            Integer speciesCount = cameraMonth.get(speciesName);
            if (speciesCount >= 0) {
                cameraMonth.merge(speciesName, 1, Integer::sum);
            } else {
                log.warn("Species detection outside camera activity interval! camera: {}, year: {}, month: {}", cameraNumber, yearMonth.getYear(), yearMonth.getMonth());
                cameraMonth.remove(speciesName);
                cameraMonth.put(speciesName, 1);
            }
        }
    }

    private void populateMatrixWithNA() {
        Set<Long> cameras = new TreeSet<>();
        Set<Integer> years = new TreeSet<>();
        csvRecords.forEach(csvRecord -> {
            cameras.add(csvRecord.getCameraNumber());
            years.add(csvRecord.getDateTime().getYear());
            speciesInOrder.add(csvRecord.getSpeciesName().toUpperCase(Locale.ROOT));
        });
        for (Long camera : cameras) {
            for (Integer year : years) {
                for (int month = 1; month <= 12; month++) {
                    TreeMap<String, Integer> speciesObservationCountMap = new TreeMap<>();
                    YearMonth actual = YearMonth.of(year, month);
                    Tuple<YearMonth, YearMonth> cameraActivityPeriod = cameraActivityPeriods.get(camera);
                    for (String speciesName : speciesInOrder) {
                        if (cameraActivityPeriod.getKey().compareTo(actual) <= 0 && cameraActivityPeriod.getValue().compareTo(actual) >= 0) {
                            speciesObservationCountMap.put(speciesName, 0);
                        } else {
                            speciesObservationCountMap.put(speciesName, -1);
                        }
                        resultMap.put(new Tuple<>(camera, actual), speciesObservationCountMap);
                    }
                }
            }
        }
    }
}
