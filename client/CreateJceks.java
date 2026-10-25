import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import javax.crypto.spec.SecretKeySpec;

public class CreateJceks {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("usage: java CreateJceks.java <file.jceks> <alias> [alias ...]");
            System.exit(1);
        }
        char[] storePass = "none".toCharArray();   // Hadoop's default provider password
        File file = new File(args[0]);

        KeyStore ks = KeyStore.getInstance("JCEKS");
        if (file.exists()) {
            try (InputStream in = new FileInputStream(file)) { ks.load(in, storePass); }
        } else {
            ks.load(null, storePass);
        }

        Console console = System.console();
        BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
        for (int i = 1; i < args.length; i++) {
            String alias = args[i].toLowerCase();
            char[] secret = (console != null)
                    ? console.readPassword("Value for %s: ", alias)
                    : stdin.readLine().toCharArray();
            SecretKeySpec key = new SecretKeySpec(
                    new String(secret).getBytes(StandardCharsets.UTF_8), "AES");
            ks.setKeyEntry(alias, key, storePass, null);   // same layout Hadoop writes
            System.out.println("stored: " + alias);
        }

        try (OutputStream out = new FileOutputStream(file)) { ks.store(out, storePass); }
    }
}
