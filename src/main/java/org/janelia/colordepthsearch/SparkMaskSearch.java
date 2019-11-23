package org.janelia.colordepthsearch;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.imageio.ImageIO;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Perform color depth mask search on a Spark cluster.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class SparkMaskSearch implements Serializable {

    private static final Logger log = LoggerFactory.getLogger(SparkMaskSearch.class);

//    private static final int ERROR_THRESHOLD = 20;
//
//    private transient final JavaSparkContext context;
//    private transient JavaPairRDD<String, ImagePlus> imagePlusRDD;
//    private Integer dataThreshold;
//    private Integer xyShift;
//    private boolean mirrorMask;
//    private Double pixColorFluctuation;
//    private Double pctPositivePixels;
//    private transient ImagePlus maskImagePlus;
//
//    public SparkMaskSearch(Integer dataThreshold, Double pixColorFluctuation, Integer xyShift,
//                           boolean mirrorMask, Double pctPositivePixels) {
//        this.dataThreshold = dataThreshold;
//        this.pixColorFluctuation = pixColorFluctuation;
//        this.xyShift = xyShift;
//        this.mirrorMask = mirrorMask;
//        this.pctPositivePixels = pctPositivePixels;
//        SparkConf conf = new SparkConf().setAppName(SparkMaskSearch.class.getName());
//        this.context = new JavaSparkContext(conf);
//    }
//

//
//    private MaskSearchResult search(String filepath, ImagePlus image, ImagePlus mask, Integer maskThreshold) {
//
//        try {
//            if (image == null) {
//                log.error("Problem loading image: {}", filepath);
//                return new MaskSearchResult(filepath, 0, 0, false, true);
//            }
//
//            log.info("Searching " + filepath);
//
//            double pixfludub = pixColorFluctuation / 100;
//
//            final ColorMIPMaskCompare cc = new ColorMIPMaskCompare(
//                    mask.getProcessor(), maskThreshold, false, null, 0,
//                    mirrorMask, dataThreshold, pixfludub, xyShift);
//            ColorMIPMaskCompare.Output output = cc.runSearch(image.getProcessor(), null);
//
//            double pixThresdub = pctPositivePixels / 100;
//            boolean isMatch = output.matchingPct > pixThresdub;
//
//            return new MaskSearchResult(filepath, output.matchingPixNum, output.matchingPct, isMatch, false);
//        }
//        catch (Throwable e) {
//            log.error("Problem searching image: {}", filepath, e);
//            return new MaskSearchResult(filepath, 0, 0, false, true);
//        }
//    }
//
//    /**
//     * Load provided image libraries into memory.
//     * @param imagesFilepath
//     */
//    public void loadImages(String imagesFilepath) throws IOException {
//
//        List<String> paths = new ArrayList<>();
//        for(String filepath : imagesFilepath.split(",")) {
//
//            Path path = Paths.get(filepath);
//            File file = path.toFile();
//            log.info("Loading image library at: {}", filepath);
//
//            if (file.isDirectory()) {
//                try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
//                    for (Path entry : stream) {
//                        paths.add(entry.toString());
//                    }
//                    log.info("  Read {} files", paths.size());
//                }
//            }
//            else {
//                paths.addAll(Files.lines(path).collect(Collectors.toList()));
//            }
//        }
//
//        // Randomize path list so that each task has some paths from each directory. Otherwise, some tasks would only get
//        // files from an "easy" directory where all the files are small
//        Collections.shuffle(paths);
//
//        log.info("Total paths: {}", paths.size());
//        log.info("Default parallelism: {}", context.defaultParallelism());
//
//        // This is a lot faster than using binaryFiles because 1) the paths are shuffled, 2) we use an optimized
//        // directory listing stream which does not consider file sizes. As a bonus, it actually respects the parallelism
//        // setting, unlike binaryFiles which ignores it unless you set other arcane settings like openCostInByte.
//        JavaRDD<String> pathRDD = context.parallelize(paths);
//        log.info("filesRdd.numPartitions: {}", pathRDD.getNumPartitions());
//
//        // This RDD is cached so that it can be reused to search with multiple masks
//        this.imagePlusRDD = pathRDD.mapToPair(filepath -> {
//            String title = new File(filepath).getName();
//            return new Tuple2<>(filepath, readImagePlus(filepath, title));
//        }).cache();
//
//        log.info("imagePlusRDD.numPartitions: {}", imagePlusRDD.getNumPartitions());
//        log.info("imagePlusRDD.count: {}", imagePlusRDD.count());
//    }
//
//    /**
//     * Perform the search.
//     * @param maskFilepath
//     * @return
//     * @throws Exception
//     */
//    public Collection<MaskSearchResult> search(String maskFilepath, Integer maskThreshold) throws Exception {
//
//        StopWatch s = new StopWatch();
//        s.start();
//
//        File maskFile = new File(maskFilepath);
//        String maskName = maskFile.getName();
//        log.info("Searching with mask: {}", maskFilepath);
//
//        // Read mask bytes in the driver
//        byte[] maskBytes = Files.readAllBytes(Paths.get(maskFilepath));
//        log.info("Loaded {} bytes for mask file", maskBytes.length);
//
//        // Send mask bytes to all workers
//        Broadcast<byte[]> maskHandle = context.broadcast(maskBytes);
//        log.info("Broadcast mask file as {}", maskHandle.id());
//
//        JavaRDD<MaskSearchResult> resultRdd = imagePlusRDD.map(pair -> {
//
//            // Cache mask object at the task level
//            if (maskImagePlus == null) {
//                byte[] bytes = maskHandle.value();
//                log.info("Got {} mask bytes", bytes.length);
//                try (ByteArrayInputStream stream = new ByteArrayInputStream(bytes)) {
//                    maskImagePlus = readImagePlus(maskFilepath, maskName, stream);
//                }
//            }
//
//            return search(pair._1, pair._2, maskImagePlus, maskThreshold);
//        });
//
//        log.info("resultRdd.numPartitions: {}", resultRdd.getNumPartitions());
//
//        JavaRDD<MaskSearchResult> sortedResultRdd = resultRdd.sortBy(MaskSearchResult::getMatchingSlices, false, 1);
//        log.info("sortedResultRdd.numPartitions: {}", sortedResultRdd.getNumPartitions());
//
//        List<MaskSearchResult> results = sortedResultRdd.collect();
//        log.info("Returning {} results", results.size());
//
//        log.info("Searching took {} ms", s.getTime());
//        return results;
//    }
//
//    private enum ImageFormat {
//        PNG,
//        TIFF,
//        UNKNOWN
//    }
//
//    private ImageFormat getImageFormat(String filepath) {
//
//        String lowerPath = filepath.toLowerCase();
//
//        if (lowerPath.endsWith(".png")) {
//            return ImageFormat.PNG;
//        }
//        else if (lowerPath.endsWith(".tiff") || lowerPath.endsWith(".tif")) {
//            return ImageFormat.TIFF;
//        }
//
//        log.info("Image format unknown: {}", filepath);
//        return ImageFormat.UNKNOWN;
//    }
//
//    public void close() {
//        if (context!=null) context.stop();
//    }
//
//    private static class Args {
//
//        @Parameter(names = {"--mask", "-m"}, description = "Image file(s) to use as the search masks", required = true, variableArity = true)
//        private List<String> maskFiles;
//
//        @Parameter(names = {"--imageDir", "-i"}, description = "Comma-delimited list of directories containing images to search", required = true)
//        private String imageDir;
//
//        @Parameter(names = {"--dataThreshold"}, description = "Data threshold")
//        private Integer dataThreshold = 100;
//
//        @Parameter(names = {"--maskThresholds"}, description = "Mask thresholds", variableArity = true)
//        private List<Integer> maskThresholds;
//
//        @Parameter(names = {"--pixColorFluctuation"}, description = "Pix Color Fluctuation, 1.18 per slice")
//        private Double pixColorFluctuation = 2.0;
//
//        @Parameter(names = {"--xyShift"}, description = "Number of pixels to try shifting in XY plane")
//        private Integer xyShift = 0;
//
//        @Parameter(names = {"--mirrorMask"}, description = "Should the mask be mirrored across the Y axis?")
//        private boolean mirrorMask = false;
//
//        @Parameter(names = {"--pctPositivePixels"}, description = "% of Positive PX Threshold (0-100%)")
//        private Double pctPositivePixels = 2.0;
//
//        @Parameter(names = {"--outputFile", "-o"}, description = "Output file(s) for results in CSV format. " +
//                "If this is not specified, the output will be printed to the log. " +
//                "If this is specified, then there should be one output file per mask file.", variableArity = true)
//        private List<String> outputFiles;
//    }
//
//    public static void main(String[] argv) throws Exception {
//
//        Args args = new Args();
//        JCommander.newBuilder()
//                .addObject(args)
//                .build()
//                .parse(argv);
//
//        Integer dataThreshold = args.dataThreshold;
//        Double pixColorFluctuation = args.pixColorFluctuation;
//        Integer xyShift = args.xyShift;
//        boolean mirrorMask = args.mirrorMask;
//        Double pctPositivePixels = args.pctPositivePixels;
//
//        if (args.maskThresholds != null) {
//            if (args.maskThresholds.size() != args.maskFiles.size()) {
//                throw new ParameterException("Number of mask thresholds must match the number of masks used");
//            }
//        }
//
//        if (args.outputFiles != null) {
//            if (args.maskFiles.size() != args.outputFiles.size()) {
//                throw new ParameterException("Number of output files must match the number of masks used");
//            }
//        }
//
//        SparkMaskSearch sparkMaskSearch = new SparkMaskSearch(
//                dataThreshold, pixColorFluctuation, xyShift, mirrorMask, pctPositivePixels);
//
//        try {
//            sparkMaskSearch.loadImages(args.imageDir);
//            int i=0;
//            for(String maskFile : args.maskFiles) {
//
//                Integer maskThreshold = 50;
//                if (args.maskThresholds != null) {
//                    maskThreshold = args.maskThresholds.get(i);
//                }
//
//                Collection<MaskSearchResult> results = sparkMaskSearch.search(maskFile, maskThreshold);
//
//                long numErrors = results.stream().filter(r -> r.isError()).count();
//
//                if (numErrors>ERROR_THRESHOLD) {
//                    throw new Exception("Number of search errors exceeded reasonable threshold ("+ERROR_THRESHOLD+")");
//                }
//                else if (numErrors>0) {
//                    log.warn("{} errors encountered while searching. These errors may represent corrupt image files:", numErrors);
//                    results.stream().filter(r -> r.isError()).forEach(r -> {
//                        log.warn("Error searching {}", r.getFilepath());
//                    });
//                }
//
//                Stream<MaskSearchResult> matchingResults = results.stream().filter(r -> r.isMatch());
//
//                if (args.outputFiles != null) {
//                    String outputFile = args.outputFiles.get(i);
//                    log.info("Writing search results for {} to {}", maskFile, outputFile);
//                    try (PrintWriter printWriter = new PrintWriter(outputFile)) {
//                        printWriter.println(maskFile);
//                        matchingResults.forEach(r -> {
//                            String filepath = r.getFilepath().replaceFirst("^file:", "");
//                            printWriter.printf("%d\t%#.5f\t%s\n", r.getMatchingSlices(), r.getMatchingSlicesPct(), filepath);
//                        });
//                    }
//                } else {
//                    log.info("Search results for {}:", maskFile);
//                    matchingResults.forEach(r -> {
//                        log.info("{} - {}", r.getMatchingSlicesPct(), r.getFilepath());
//                    });
//                }
//
//                i++;
//            }
//        }
//        finally {
//            sparkMaskSearch.close();
//        }
//    }

}
