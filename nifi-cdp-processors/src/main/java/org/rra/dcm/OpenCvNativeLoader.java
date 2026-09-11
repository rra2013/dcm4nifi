package org.rra.dcm;

import org.opencv.core.Core;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Locale;

public final class OpenCvNativeLoader {

    private static volatile boolean loaded;
    private static Path extractedLibrary;

    private OpenCvNativeLoader() {
    }

    /**
     * Lädt OpenCV genau einmal pro ClassLoader.
     *
     * Reihenfolge:
     * 1. java.library.path / PATH / LD_LIBRARY_PATH
     * 2. Native Library aus den NAR-Ressourcen
     */
    public static void load() {
        if (loaded) {
            return;
        }

        synchronized (OpenCvNativeLoader.class) {
            if (loaded) {
                return;
            }

            UnsatisfiedLinkError systemLoadError = null;

            /*
             * Unter Windows wird beispielsweise opencv_java.dll gesucht,
             * unter Linux libopencv_java.so.
             */
            try {
                System.loadLibrary(Core.NATIVE_LIBRARY_NAME);
                loaded = true;
                return;
            } catch (UnsatisfiedLinkError e) {
                systemLoadError = e;
            } catch (SecurityException e) {
                throw new IllegalStateException(
                        "Keine Berechtigung zum Laden der OpenCV-Bibliothek "
                                + Core.NATIVE_LIBRARY_NAME,
                        e
                );
            }

            String nativeSpecification = getNativeSpecification();
            String libraryFileName =
                    System.mapLibraryName(Core.NATIVE_LIBRARY_NAME);

            String resourcePath = "/native/"
                    + nativeSpecification
                    + "/"
                    + libraryFileName;

            try {
                extractedLibrary = extractLibrary(resourcePath, libraryFileName);

                System.load(extractedLibrary.toAbsolutePath().toString());

                loaded = true;
            } catch (IOException | UnsatisfiedLinkError | SecurityException e) {
                IllegalStateException exception = new IllegalStateException(
                        "OpenCV konnte nicht geladen werden."
                                + System.lineSeparator()
                                + "Library name: " + Core.NATIVE_LIBRARY_NAME
                                + System.lineSeparator()
                                + "Betriebssystem/Architektur: " + nativeSpecification
                                + System.lineSeparator()
                                + "java.library.path: "
                                + System.getProperty("java.library.path")
                                + System.lineSeparator()
                                + "Resource: " + resourcePath,
                        e
                );

                if (systemLoadError != null) {
                    exception.addSuppressed(systemLoadError);
                }

                throw exception;
            }
        }
    }

    public static boolean isLoaded() {
        return loaded;
    }

    public static Path getExtractedLibrary() {
        return extractedLibrary;
    }

    private static Path extractLibrary(
            String resourcePath,
            String libraryFileName
    ) throws IOException {

        try (InputStream input =
                     OpenCvNativeLoader.class.getResourceAsStream(resourcePath)) {

            if (input == null) {
                throw new IOException(
                        "Native OpenCV-Bibliothek nicht in den Ressourcen gefunden: "
                                + resourcePath
                );
            }

            Path temporaryDirectory =
                    Files.createTempDirectory("dcm4che-opencv-");

            Path temporaryLibrary =
                    temporaryDirectory.resolve(libraryFileName);

            Files.copy(
                    input,
                    temporaryLibrary,
                    StandardCopyOption.REPLACE_EXISTING
            );

            temporaryLibrary.toFile().deleteOnExit();
            temporaryDirectory.toFile().deleteOnExit();

            return temporaryLibrary;
        }
    }

    private static String getNativeSpecification() {
        String os = normalizeOperatingSystem(
                System.getProperty("os.name", "")
        );

        String architecture = normalizeArchitecture(
                System.getProperty("os.arch", "")
        );

        return os + "-" + architecture;
    }

    private static String normalizeOperatingSystem(String osName) {
        String value = osName.toLowerCase(Locale.ROOT);

        if (value.startsWith("win")) {
            return "windows";
        }

        if (value.startsWith("mac") || value.startsWith("darwin")) {
            return "macosx";
        }

        if (value.startsWith("linux")) {
            return "linux";
        }

        throw new IllegalStateException(
                "Nicht unterstütztes Betriebssystem: " + osName
        );
    }

    private static String normalizeArchitecture(String osArchitecture) {
        String value = osArchitecture.toLowerCase(Locale.ROOT);

        switch (value) {
            case "amd64":
            case "x86_64":
            case "x86-64":
            case "em64t":
                return "x86-64";

            case "aarch64":
            case "arm64":
                return "aarch64";

            case "x86":
            case "i386":
            case "i486":
            case "i586":
            case "i686":
                return "x86";

            default:
                throw new IllegalStateException(
                        "Nicht unterstützte Prozessorarchitektur: "
                                + osArchitecture
                );
        }
    }
}