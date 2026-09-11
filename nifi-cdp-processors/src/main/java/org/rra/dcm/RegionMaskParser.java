package org.rra.dcm;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class RegionMaskParser {

    private static final Pattern REGION_PATTERN = Pattern.compile(
            "\\[\\s*(\\d+)\\s*," +
                    "\\s*(\\d+)\\s*," +
                    "\\s*(\\d+)\\s*," +
                    "\\s*(\\d+)\\s*\\]"
    );

    private RegionMaskParser() {
    }

    public static ArrayList<MaskRegion> parse(String input) {
        if (input == null || input.isBlank()) {
            throw new IllegalArgumentException(
                    "Die Regions-Eingabe darf nicht leer sein");
        }

        ArrayList<MaskRegion> regions = new ArrayList<>();
        Matcher matcher = REGION_PATTERN.matcher(input);

        int previousEnd = 0;

        while (matcher.find()) {
            validateSeparator(
                    input.substring(previousEnd, matcher.start()));

            int x = parseInteger(matcher.group(1), "x");
            int y = parseInteger(matcher.group(2), "y");
            int width = parseInteger(matcher.group(3), "width");
            int height = parseInteger(matcher.group(4), "height");

            regions.add(new MaskRegion(
                    x,
                    y,
                    width,
                    height
            ));

            previousEnd = matcher.end();
        }

        validateSeparator(input.substring(previousEnd));

        if (regions.isEmpty()) {
            throw new IllegalArgumentException(
                    "Ungültiges Format. Erwartet wird beispielsweise "
                            + "[10,20,100,50]");
        }

        return regions;
    }

    private static int parseInteger(
            String value,
            String propertyName
    ) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Ungültiger Wert für " + propertyName + ": " + value,
                    e
            );
        }
    }

    private static void validateSeparator(String separator) {
        /*
         * Zwischen den Regionen sind nur Leerzeichen und Kommas erlaubt.
         */
        if (!separator.matches("\\s*,?\\s*")) {
            throw new IllegalArgumentException(
                    "Ungültiges Regions-Format");
        }
    }
}