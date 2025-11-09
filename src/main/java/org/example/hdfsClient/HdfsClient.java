package org.example.hdfsClient;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Scanner;

// Hadoop imports (для FileSystem API)
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * HDFS Client with dual-mode support:
 * - WebHDFS REST API (default, per assignment)
 * - Java FileSystem API (from Hadoop SDK)
 * <p>
 * Usage:
 * java HdfsClient [-web | -fs] <host> <port> <username>
 * <p>
 * Examples:
 * java HdfsClient -web localhost 50070 aslebedev
 * java HdfsClient -fs localhost 9000 aslebedev
 * <p>
 * Note: For -fs mode, port is usually 9000 (HDFS RPC), not 50070 (WebHDFS HTTP).
 */
public class HdfsClient {
    private final String host;
    private final int port;
    private final String username;
    private final boolean useFileSystemApi;

    private static String currentHdfsDir = "/user/";
    private String currentLocalDir = System.getProperty("user.dir");

    private FileSystem hdfsFs = null;

    public HdfsClient(String host, int port, String username, boolean useFileSystemApi) throws IOException, InterruptedException {
        this.host = host;
        this.port = port;
        this.username = username;
        this.useFileSystemApi = useFileSystemApi;

        if (useFileSystemApi) {
            Configuration conf = new Configuration();
            String hdfsUri = "hdfs://" + host + ":" + port;
            conf.set("fs.defaultFS", hdfsUri);
            conf.setInt("dfs.replication", 1);
            conf.setBoolean("dfs.permissions.enabled", false);
            this.hdfsFs = FileSystem.get(URI.create(hdfsUri), conf, username);
            System.out.println("Connected via Java FileSystem API to: " + hdfsUri);
        } else {
            System.out.println("Using WebHDFS REST API on http://" + host + ":" + port);
        }
    }

    private String buildWebHdfsUrl(String path, String operation, String... params) {
        StringBuilder url = new StringBuilder();
        url.append("http://").append(host).append(":").append(port)
                .append("/webhdfs/v1").append(path)
                .append("?user.name=").append(username)
                .append("&op=").append(operation);
        for (String param : params) {
            url.append("&").append(param);
        }
        return url.toString();
    }

    private void sendHttpPutRequest(String urlString) throws IOException {
        URL url = new URL(urlString);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("PUT");
        int code = conn.getResponseCode();
        if (code != 200 && code != 201) {
            throw new IOException("HTTP " + code + " for: " + urlString);
        }
        conn.disconnect();
    }

    private void sendHttpDeleteRequest(String urlString) throws IOException {
        URL url = new URL(urlString);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("DELETE");
        int code = conn.getResponseCode();
        if (code != 200) {
            throw new IOException("HTTP " + code + " for: " + urlString);
        }
        conn.disconnect();
    }

    public void mkdir(String dirName) throws IOException {
        String fullPath = normalizePath(currentHdfsDir, dirName);
        if (useFileSystemApi) {
            Path path = new Path(fullPath);
            hdfsFs.mkdirs(path, FsPermission.getDirDefault());
        } else {
            String url = buildWebHdfsUrl(fullPath, "MKDIRS");
            sendHttpPutRequest(url);
        }
        System.out.println("Directory created: " + fullPath);
    }

    public void put(String localFileName) throws IOException {
        String hdfsPath = normalizePath(currentHdfsDir, localFileName);
        if (useFileSystemApi) {
            Path src = new Path(localFileName);
            Path dst = new Path(hdfsPath);
            hdfsFs.copyFromLocalFile(src, dst);
        } else {
            String createUrl = buildWebHdfsUrl(hdfsPath, "CREATE", "overwrite=true");
            HttpURLConnection nnConn = (HttpURLConnection) new URL(createUrl).openConnection();
            nnConn.setRequestMethod("PUT");
            nnConn.setInstanceFollowRedirects(false);
            int code = nnConn.getResponseCode();
            if (code == 307) {
                String redirect = nnConn.getHeaderField("Location");
                nnConn.disconnect();

                HttpURLConnection uploadConn = (HttpURLConnection) new URL(redirect).openConnection();
                uploadConn.setRequestMethod("PUT");
                uploadConn.setDoOutput(true);
                uploadConn.setRequestProperty("Content-Type", "application/octet-stream");

                byte[] data = Files.readAllBytes(Paths.get(localFileName));
                try (OutputStream os = uploadConn.getOutputStream()) {
                    os.write(data);
                    os.flush();
                }

                int uploadCode = uploadConn.getResponseCode();
                if (uploadCode != HttpURLConnection.HTTP_CREATED && uploadCode != HttpURLConnection.HTTP_OK) {
                    throw new IOException("UPLOAD failed: HTTP " + uploadCode);
                }
                uploadConn.disconnect();
            } else if (code == HttpURLConnection.HTTP_CREATED || code == HttpURLConnection.HTTP_OK) {
                nnConn.disconnect();
            } else {
                nnConn.disconnect();
                throw new IOException("CREATE failed: HTTP " + code);
            }
        }
        System.out.println("Uploaded: " + localFileName + " -> " + hdfsPath);
    }

    public void get(String hdfsFileName) throws IOException {
        String hdfsPath = normalizePath(currentHdfsDir, hdfsFileName);
        File localFile = new File(currentLocalDir, hdfsFileName);
        if (useFileSystemApi) {
            Path src = new Path(hdfsPath);
            Path dst = new Path(localFile.getAbsolutePath());
            hdfsFs.copyToLocalFile(false, src, dst, true);
        } else {
            String openUrl = buildWebHdfsUrl(hdfsPath, "OPEN");
            HttpURLConnection conn = (HttpURLConnection) new URL(openUrl).openConnection();
            conn.setRequestMethod("GET");
            conn.setInstanceFollowRedirects(false);
            int code = conn.getResponseCode();
            if (code != 307) throw new IOException("OPEN failed: " + code);
            String redirect = conn.getHeaderField("Location");
            conn.disconnect();

            try (InputStream is = new URL(redirect).openConnection().getInputStream();
                 FileOutputStream fos = new FileOutputStream(hdfsFileName)) {

                byte[] buf = new byte[8192];
                int n;
                while ((n = is.read(buf)) != -1) {
                    fos.write(buf, 0, n);
                }
                fos.flush();
            }

        }
        System.out.println("Downloaded: " + hdfsPath + " -> " + hdfsFileName);
    }

    public void append(String localFileName, String hdfsFileName) throws IOException {
        String hdfsPath = normalizePath(currentHdfsDir, hdfsFileName);
        if (useFileSystemApi) {
            try (FSDataOutputStream out = hdfsFs.append(new Path(hdfsPath))) {
                byte[] data = Files.readAllBytes(Paths.get(localFileName));
                out.write(data);
                out.hflush();
            }
        } else {
            String appendUrl = buildWebHdfsUrl(hdfsPath, "APPEND");
            HttpURLConnection nnConn = (HttpURLConnection) new URL(appendUrl).openConnection();
            nnConn.setRequestMethod("POST");
            nnConn.setInstanceFollowRedirects(false);
            int code = nnConn.getResponseCode();
            if (code == 307) {
                String redirect = nnConn.getHeaderField("Location");
                nnConn.disconnect();

                HttpURLConnection uploadConn = (HttpURLConnection) new URL(redirect).openConnection();
                uploadConn.setRequestMethod("POST");
                uploadConn.setDoOutput(true);
                uploadConn.setRequestProperty("Content-Type", "application/octet-stream");

                byte[] data = Files.readAllBytes(Paths.get(localFileName));
                try (OutputStream os = uploadConn.getOutputStream()) {
                    os.write(data);
                    os.flush();
                }

                int uploadCode = uploadConn.getResponseCode();
                if (uploadCode != HttpURLConnection.HTTP_OK && uploadCode != HttpURLConnection.HTTP_CREATED) {
                    throw new IOException("APPEND failed: HTTP " + uploadCode);
                }
                uploadConn.disconnect();
            } else {
                nnConn.disconnect();
                throw new IOException("APPEND failed: HTTP " + code);
            }
        }
        System.out.println("Appended: " + localFileName + " -> " + hdfsPath);
    }

    public void delete(String pathName) throws IOException {
        String fullPath = normalizePath(currentHdfsDir, pathName);
        if (useFileSystemApi) {
            hdfsFs.delete(new Path(fullPath), true);
        } else {
            String url = buildWebHdfsUrl(fullPath, "DELETE", "recursive=true");
            sendHttpDeleteRequest(url);
        }
        System.out.println("Deleted: " + fullPath);
    }

    public void ls() throws IOException {
        if (useFileSystemApi) {
            FileStatus[] statuses = hdfsFs.listStatus(new Path(currentHdfsDir));
            System.out.println("Contents of " + currentHdfsDir + ":");
            for (FileStatus s : statuses) {
                System.out.println((s.isDirectory() ? "D " : "F ") + s.getPath().getName());
            }
        } else {
            String urlStr = buildWebHdfsUrl(currentHdfsDir, "LISTSTATUS");
            URL url = new URL(urlStr);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            int code = conn.getResponseCode();
            if (code != 200) throw new IOException("LISTSTATUS failed: " + code);

            try (BufferedReader r = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                String json = r.lines().collect(StringBuilder::new, StringBuilder::append, StringBuilder::append).toString();
                if (json.contains("\"FileStatuses\":{\"FileStatus\":[")) {
                    String files = json.split("\"FileStatuses\":\\{\"FileStatus\":\\[")[1].split("\\]\\}")[0];
                    String[] entries = files.split("\\},\\{");
                    System.out.println("Contents of " + currentHdfsDir + ":");
                    for (String e : entries) {
                        boolean isDir = e.contains("\"type\":\"DIRECTORY\"");
                        String name = e.contains("\"pathSuffix\":\"")
                                ? e.split("\"pathSuffix\":\"")[1].split("\"")[0] : "";
                        if (!name.isEmpty()) System.out.println((isDir ? "D " : "F ") + name);
                    }
                }
            }
        }
    }

    public void cd(String dirName) {
        if ("..".equals(dirName)) {
            if (!"/".equals(currentHdfsDir)) {
                int lastSlash = currentHdfsDir.lastIndexOf('/');
                currentHdfsDir = (lastSlash <= 0) ? "/" : currentHdfsDir.substring(0, lastSlash);
            }
        } else {
            currentHdfsDir = normalizePath(currentHdfsDir, dirName);
        }
        System.out.println("Current HDFS dir: " + currentHdfsDir);
    }

    public void lls() {
        File[] files = new File(currentLocalDir).listFiles();
        if (files != null) {
            System.out.println("Local contents of " + currentLocalDir + ":");
            for (File f : files) {
                System.out.println((f.isDirectory() ? "D " : "F ") + f.getName());
            }
        }
    }

    public void lcd(String dirName) {
        File target = "..".equals(dirName)
                ? new File(currentLocalDir).getParentFile()
                : new File(currentLocalDir, dirName);
        if (target != null && target.exists() && target.isDirectory()) {
            currentLocalDir = target.getAbsolutePath();
            System.out.println("Current local dir: " + currentLocalDir);
        } else {
            System.out.println("Local dir not found: " + (target != null ? target.getAbsolutePath() : dirName));
        }
    }

    private String normalizePath(String base, String name) {
        if ("/".equals(base)) return "/" + name;
        return base + "/" + name;
    }

    public void close() throws IOException {
        if (hdfsFs != null) hdfsFs.close();
    }

    public void executeCommand(String input) {
        if (input == null || input.trim().isEmpty()) return;
        String[] parts = input.trim().split("\\s+");
        String cmd = parts[0].toLowerCase();

        try {
            switch (cmd) {
                case "mkdir":
                    validateArgs(parts, 2);
                    mkdir(parts[1]);
                    break;
                case "put":
                    validateArgs(parts, 2);
                    put(parts[1]);
                    break;
                case "get":
                    validateArgs(parts, 2);
                    get(parts[1]);
                    break;
                case "append":
                    validateArgs(parts, 3);
                    append(parts[1], parts[2]);
                    break;
                case "delete":
                    validateArgs(parts, 2);
                    delete(parts[1]);
                    break;
                case "ls":
                    validateArgs(parts, 1);
                    ls();
                    break;
                case "cd":
                    validateArgs(parts, 2);
                    cd(parts[1]);
                    break;
                case "lls":
                    validateArgs(parts, 1);
                    lls();
                    break;
                case "lcd":
                    validateArgs(parts, 2);
                    lcd(parts[1]);
                    break;
                case "exit":
                case "quit":
                    System.out.println("Bye!");
                    System.exit(0);
                default:
                    System.out.println("Unknown command: " + cmd);
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    private void validateArgs(String[] parts, int expected) {
        if (parts.length != expected) {
            throw new IllegalArgumentException("Wrong number of arguments");
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3 || args.length > 4) {
            System.err.println("Usage: java HdfsClient [-web | -fs] <host> <port> <username>");
            System.exit(1);
        }

        boolean useFs = false;
        int offset = 0;
        if (args[0].equals("-fs")) {
            useFs = true;
            offset = 1;
        } else if (args[0].equals("-web")) {
            useFs = false;
            offset = 1;
        }

        String host = args[offset];
        int port = Integer.parseInt(args[offset + 1]);
        String user = args[offset + 2];
        currentHdfsDir = currentHdfsDir.concat(user);

        HdfsClient client = new HdfsClient(host, port, user, useFs);
        Scanner sc = new Scanner(System.in);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                client.close();
            } catch (IOException ignored) {
            }
        }));

        System.out.println("HDFS Client ready. Mode: " + (useFs ? "FileSystem API" : "WebHDFS"));
        while (true) {
            System.out.print("hdfs> ");
            client.executeCommand(sc.nextLine());
        }
    }
}