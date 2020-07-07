package org.janelia.colordepthsearch;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.base.Splitter;
import com.google.common.collect.Streams;

import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.colormipsearch.api.cdsearch.ColorMIPCompareOutput;
import org.janelia.colormipsearch.api.cdsearch.ColorMIPMaskCompare;
import org.janelia.colormipsearch.api.cdsearch.ColorMIPSearch;
import org.janelia.colormipsearch.api.cdsearch.ColorMIPSearchResult;
import org.janelia.colormipsearch.api.cdmips.MIPImage;
import org.janelia.colormipsearch.api.cdmips.MIPMetadata;
import org.janelia.colormipsearch.api.cdmips.MIPsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AWSLambdaColorMIPSearch {

    private static final Logger LOG = LoggerFactory.getLogger(MIPsUtils.class);

    private final AWSMIPLoader mipLoader;
    private final ColorMIPSearch colorMIPSearch;
    private final String awsMasksBucket;
    private final String awsLibrariesBucket;
    private final String awsLibrariesThumbnailsBucket;

    AWSLambdaColorMIPSearch(AWSMIPLoader mipLoader,
                            ColorMIPSearch colorMIPSearch,
                            String awsMasksBucket,
                            String awsLibrariesBucket,
                            String awsLibrariesThumbnailsBucket) {
        this.mipLoader = mipLoader;
        this.colorMIPSearch = colorMIPSearch;
        this.awsMasksBucket = awsMasksBucket;
        this.awsLibrariesBucket = awsLibrariesBucket;
        this.awsLibrariesThumbnailsBucket = awsLibrariesThumbnailsBucket;
    }

    List<ColorMIPSearchResult> findAllColorDepthMatches(List<String> maskKeys,
                                                        List<Integer> maskThresholds,
                                                        List<String> libraryKeys) {
        return Streams.zip(maskKeys.stream(), maskThresholds.stream(),
                (maskKey, maskThreshold) -> runMaskSearches(maskKey, maskThreshold, libraryKeys))
                .flatMap(cdsResults -> cdsResults.stream())
                .collect(Collectors.toList());
    }

    private List<ColorMIPSearchResult> runMaskSearches(String maskKey, int maskThreshold, List<String> libraryKeys) {
        MIPMetadata maskMIP = createMaskMIP(maskKey);
        MIPImage maskImage = mipLoader.loadMIP(awsMasksBucket, maskMIP);
        if (maskImage == null) {
            return Collections.emptyList();
        }
        ColorMIPMaskCompare maskComparator = colorMIPSearch.createMaskComparator(maskImage, maskThreshold);
        return libraryKeys.stream()
                .map(this::createLibraryMIP)
                .map(libraryMIP -> mipLoader.loadMIP(awsLibrariesBucket, libraryMIP))
                .filter(libraryImage -> libraryImage != null)
                .map(libraryImage -> {
                    LOG.trace("Compare {} with {}", maskImage, libraryImage);
                    ColorMIPCompareOutput sr = maskComparator.runSearch(libraryImage.getImageArray());
                    if (colorMIPSearch.isMatch(sr)) {
                        return new ColorMIPSearchResult(
                                maskMIP,
                                libraryImage.getMipInfo(),
                                sr.getMatchingPixNum(),
                                sr.getMatchingPixNumToMaskRatio(),
                                true,
                                false
                        );
                    } else {
                        return new ColorMIPSearchResult(
                                maskMIP,
                                libraryImage.getMipInfo(),
                                0, 0, false, false);
                    }
                })
                .filter(ColorMIPSearchResult::isMatch)
                .collect(Collectors.toList());
    }

    private MIPMetadata createMaskMIP(String mipKey) {
        Path mipPath = Paths.get(mipKey);
        String mipNameComponent = mipPath.getFileName().toString();
        String mipName = RegExUtils.replacePattern(mipNameComponent, "\\..*$", "");
        LOG.info("!!!!!!!!!!!!!!! MASK MIP NAME {} -> {}", mipNameComponent, mipName);
        MIPMetadata mip = new MIPMetadata();
        mip.setId(mipName);
        mip.setCdmPath(mipKey);
        mip.setImageName(mipKey);
        return mip;
    }

    private MIPMetadata createLibraryMIP(String mipKey) {
        Path mipPath = Paths.get(mipKey);
        String mipNameComponent = mipPath.getFileName().toString();
        String mipName = RegExUtils.replacePattern(mipNameComponent, "\\..*$", "");
        String mipThumbnailKey = RegExUtils.replacePattern(mipKey, "\\..*$", ".jpg");
        int nPathComponents = mipPath.getNameCount();
        MIPMetadata mip = new MIPMetadata();
        mip.setId(mipName);
        mip.setCdmPath(mipKey);
        mip.setImageName(mipKey);
        mip.setImageURL(String.format("https://s3.amazonaws.com/%s/%s", awsLibrariesBucket, mipKey));
        mip.setThumbnailURL(String.format("https://s3.amazonaws.com/%s/%s", awsLibrariesThumbnailsBucket, mipThumbnailKey));
        if (nPathComponents > 2) {
            mip.setLibraryName(mipPath.getName(nPathComponents - 2).toString());
        }
        if (nPathComponents > 3) {
            mip.setAlignmentSpace(mipPath.getName(nPathComponents - 3).toString());
        }
        if (isEmLibrary(mip.getLibraryName())) {
            populateEMMetadataFromName(mipName, mip);
        } else {
            populateLMMetadataFromName(mipName, mip);
        }
        return mip;
    }

    private boolean isEmLibrary(String lname) {
        return lname != null && StringUtils.containsIgnoreCase(lname, "flyem") && StringUtils.containsIgnoreCase(lname, "hemibrain");
    }

    private void populateLMMetadataFromName(String mipName, MIPMetadata mipMetadata) {
        List<String> mipNameComponents = Splitter.on('-').splitToList(mipName);
        String line = mipNameComponents.size() > 0 ? mipNameComponents.get(0) : mipName;
        // attempt to remove the PI initials
        int piSeparator = StringUtils.indexOf(line, '_');
        String lineID;
        if (piSeparator == -1) {
            lineID = line;
        } else {
            lineID = line.substring(piSeparator + 1);
        }
        mipMetadata.setPublishedName(lineID);
        if (mipNameComponents.size() >= 2) {
            mipMetadata.setSlideCode(mipNameComponents.get(1));
        }
        if (mipNameComponents.size() >= 4) {
            mipMetadata.setGender(mipNameComponents.get(3));
        }
        if (mipNameComponents.size() >= 5) {
            mipMetadata.setObjective(mipNameComponents.get(4));
        }
        if (mipNameComponents.size() >= 6) {
            mipMetadata.setAnatomicalArea(mipNameComponents.get(5));
        }
    }

    private void populateEMMetadataFromName(String mipName, MIPMetadata mipMetadata) {
        List<String> mipNameComponents = Splitter.on('-').splitToList(mipName);
        String bodyID = mipNameComponents.size() > 0 ? mipNameComponents.get(0) : mipName;
        mipMetadata.setPublishedName(bodyID);
    }

}
