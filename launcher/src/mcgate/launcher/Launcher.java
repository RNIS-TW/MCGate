package mcgate.launcher;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * `java -jar mcgate.jar [CONFIG [MESSAGES]]` - runs the native mcgate binary bundled inside this jar.
 *
 * Exists for hosts that can only launch a jar (Pterodactyl-style panels, shared Minecraft hosting).
 * The jar carries one binary per platform under /natives/&lt;target triple&gt;/ (see build-jar.sh); this
 * picks the one matching the running OS/arch, extracts it to .run/bin/ in the working directory,
 * and execs it with the JVM's own stdin/stdout/stderr, so the console and log output behave exactly
 * as if the binary had been started directly. Working directory and arguments pass through
 * untouched, so config.yml/messages.yml/log/ land where they would without the jar.
 *
 * Shutdown: the binary's graceful path (kick players, flush SQLite) is only wired to SIGINT and the
 * console `stop` command, while a panel or `kill` stops the JVM with SIGTERM. The shutdown hook
 * therefore translates JVM shutdown into SIGINT for the child, waits, and only then escalates.
 */
public final class Launcher {
    private static final String APP = "mcgate";
    private static final long GRACEFUL_STOP_SECONDS = 20;

    public static void main(String[] args) throws Exception {
        Path binary = resolveBinary();
        List<String> command = new ArrayList<>();
        command.add(binary.toString());
        command.addAll(Arrays.asList(args));

        Process child = new ProcessBuilder(command).inheritIO().start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> stopChild(child), "mcgate-shutdown"));
        System.exit(child.waitFor());
    }

    /** $MCGATE_BIN (or -Dmcgate.bin) overrides the embedded binary, e.g. to test a local build. */
    private static Path resolveBinary() throws IOException {
        String override = System.getProperty("mcgate.bin", System.getenv("MCGATE_BIN"));
        if (override != null && !override.isEmpty()) {
            Path p = Paths.get(override);
            if (!Files.isExecutable(p)) fail("MCGATE_BIN=" + override + " is not executable");
            return p;
        }

        List<String> triples = targetTriples();
        String name = null;
        byte[] bytes = null;
        for (String triple : triples) {
            String candidate = triple.contains("windows") ? APP + ".exe" : APP;
            try (InputStream in = Launcher.class.getResourceAsStream("/natives/" + triple + "/" + candidate)) {
                if (in == null) continue;
                name = candidate;
                bytes = in.readAllBytes();
                break;
            }
        }
        if (bytes == null) fail("this jar has no binary for this platform (tried " + String.join(", ", triples) + ")");

        // Name the extracted file by content hash: an unchanged jar reuses the existing file, and an
        // upgraded jar never tries to overwrite a binary that a still-running instance has open
        // (which Windows refuses outright).
        Path dir = Paths.get(".run", "bin").toAbsolutePath();
        Files.createDirectories(dir);
        Path target = dir.resolve(APP + "-" + sha256(bytes).substring(0, 12) + (name.endsWith(".exe") ? ".exe" : ""));
        if (!Files.exists(target) || Files.size(target) != bytes.length) {
            Path tmp = Files.createTempFile(dir, APP, ".tmp");
            Files.write(tmp, bytes);
            tmp.toFile().setExecutable(true, true);
            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        }
        if (!Files.isExecutable(target) && !target.toFile().setExecutable(true, true)) {
            fail("cannot make " + target + " executable (is the filesystem mounted noexec?)");
        }
        return target;
    }

    /**
     * Maps the JVM's os.name/os.arch onto the Rust target triples build.sh produces, best match
     * first; the first one the jar actually contains wins. Linux prefers the static musl builds,
     * which run on any distro (Alpine included, and glibc too old for the gnu builds). The
     * fallbacks rely on the OS running the other binary for us: Rosetta 2 runs x86_64 macOS
     * binaries on Apple Silicon, Windows on ARM emulates x64/x86, and 64-bit x86 Linux/Windows run
     * 32-bit x86 binaries natively.
     */
    private static List<String> targetTriples() {
        String os = System.getProperty("os.name").toLowerCase(Locale.ROOT);
        String arch = System.getProperty("os.arch").toLowerCase(Locale.ROOT);
        boolean arm64 = arch.equals("aarch64") || arch.equals("arm64");
        boolean x64 = arch.equals("amd64") || arch.equals("x86_64");
        boolean x86 = arch.equals("x86") || arch.equals("i386") || arch.equals("i486") || arch.equals("i586") || arch.equals("i686");
        boolean arm32 = arch.equals("arm") || arch.startsWith("armv7") || arch.equals("armhf");
        if (os.contains("mac") || os.contains("darwin")) {
            if (arm64) return List.of("aarch64-apple-darwin", "x86_64-apple-darwin");
            if (x64) return List.of("x86_64-apple-darwin");
        } else if (os.contains("linux")) {
            if (x64) return List.of("x86_64-unknown-linux-musl", "x86_64-unknown-linux-gnu", "i686-unknown-linux-musl");
            if (arm64) return List.of("aarch64-unknown-linux-musl", "aarch64-unknown-linux-gnu", "armv7-unknown-linux-musleabihf");
            if (x86) return List.of("i686-unknown-linux-musl");
            if (arm32) return List.of("armv7-unknown-linux-musleabihf");
        } else if (os.contains("windows")) {
            if (x64) return List.of("x86_64-pc-windows-gnu", "i686-pc-windows-gnu");
            if (arm64) return List.of("x86_64-pc-windows-gnu", "i686-pc-windows-gnu");
            if (x86) return List.of("i686-pc-windows-gnu");
        }
        fail("unsupported platform: " + os + "/" + arch);
        return null; // unreachable
    }

    private static void stopChild(Process child) {
        if (!child.isAlive()) return;
        try {
            if (!System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("windows")) {
                // Ctrl+C in a terminal already delivered SIGINT to the child too (same process group);
                // a second one is harmless - tokio's handler stays installed after the first.
                new ProcessBuilder("kill", "-INT", Long.toString(child.pid())).inheritIO().start().waitFor();
                if (child.waitFor(GRACEFUL_STOP_SECONDS, TimeUnit.SECONDS)) return;
                System.err.println(APP + ": still running after " + GRACEFUL_STOP_SECONDS + "s, sending SIGTERM");
            }
            child.destroy();
            if (child.waitFor(5, TimeUnit.SECONDS)) return;
            child.destroyForcibly().waitFor(5, TimeUnit.SECONDS);
        } catch (InterruptedException | IOException e) {
            child.destroyForcibly();
        }
    }

    private static String sha256(byte[] bytes) {
        try {
            StringBuilder hex = new StringBuilder();
            for (byte b : MessageDigest.getInstance("SHA-256").digest(bytes)) hex.append(String.format("%02x", b));
            return hex.toString();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static void fail(String message) {
        System.err.println(APP + ": " + message);
        System.exit(1);
    }
}
