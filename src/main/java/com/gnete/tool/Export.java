package com.gnete.tool;

import java.util.Properties;
import java.util.Scanner;
import java.io.*;
import java.sql.*;
import net.sf.json.*;

public class Export {

    private static String dbClass;
    private static String dbUrl;
    private static String host;
    private static short port;
    private static String db;
    private static String user;
    private static String password;
    
    private static String sql;
    private static JSONArray params;
    private static String output;
    private static String outputCharset;
    private static boolean showHeader = false;
    private static String splitText;
    private static int splitLength;
    private static String lineBreak = "\r\n", nullStr = "";
    
    private static final String UTF8 = "UTF-8";
    
    private static String readSql(String fileName, String charset)
            throws IOException {
        Scanner scanner = new Scanner(new File(fileName), charset);
        StringBuilder buffer = new StringBuilder();
        while (scanner.hasNextLine()) {
            String temp = scanner.nextLine().trim();
            if (temp.length() != 0) {
                buffer.append(' ');
                buffer.append(temp);
            }
        }
        if (buffer.charAt(buffer.length() - 1) == ';') {
            return buffer.substring(1, buffer.length() - 1);
        }
        return buffer.substring(1);
    }

    private static boolean getInfo(String[] args) {
        if (args.length < 1) {
            System.out.println("Example:");
            System.out.println("java -jar Exporter.jar config.properties");
        }
        try {
            Properties properties = new Properties();
            FileInputStream inputStream = new FileInputStream(args[0]);
            properties.load(inputStream);
            inputStream.close();
            
            host = properties.getProperty("host", "127.0.0.1");
            port = Short.parseShort(properties.getProperty("port", "1521"));
            db = properties.getProperty("db", "");
            user = properties.getProperty("user", "");
            password = properties.getProperty("password", "");
            
            setAccordingToDbType(properties);
            
            String inputCharset = properties.getProperty("sql.charset", "UTF-8");
            sql = readSql(properties.getProperty("sql", "sql.sql"), inputCharset);
            if ("true".equals(System.getProperty("debug"))) {
                System.out.println(sql);
            }
            String paramsJson = properties.getProperty("params", "");
            if (paramsJson.length() > 0) {
                params = JSONArray.fromObject(paramsJson);
            } else {
                params = new JSONArray();
            }
            output = properties.getProperty("output", "result.txt");
            outputCharset = properties.getProperty("output.charset", "UTF-8");
            
            splitText = properties.getProperty("split", "\t");
            splitLength = splitText.length();
            
            String header = properties.getProperty("header", "0");
            showHeader = "1".equals(header) || "true".equals(header) ||
                    "True".equals(header) || "TRUE".equals(header);
            
            String nullStr = properties.getProperty("null_str");
            String lineFeed = properties.getProperty("line_feed");
            if ("\\r".equals(lineFeed)) {
                lineBreak = "\r";
            } else if ("\\n".equals(lineFeed)) {
                lineBreak = "\n";
            }
            return true;
        } catch (Exception ex) {
            ex.printStackTrace();
            return false;
        }
    }
    
    private static void setAccordingToDbType(Properties properties) {
        String dbType = properties.getProperty("db_type", "oracle");
        if ("mysql".equalsIgnoreCase(dbType)) {
            dbClass = "com.mysql.jdbc.Driver";
            dbUrl = "jdbc:mysql://" + host + ':' + port + '/' + db;
        } else if ("mssql".equalsIgnoreCase(dbType)) {
            dbClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
            dbUrl = "jdbc:sqlserver://" + host + ':' + port + ";DatabaseName=" + db;
        } else {
            dbClass = "oracle.jdbc.driver.OracleDriver";
            dbUrl = "jdbc:oracle:thin:@" + host + ':' + port + ':' + db;
        }
    }
    
    private static void setParam(PreparedStatement stmt, int index, Object param)
            throws SQLException {
        if (null == param) {
            stmt.setNull(index, Types.NULL);
        } else if (param instanceof Integer) {
            stmt.setInt(index, Integer.class.cast(param));
        } else if (param instanceof Long) {
            stmt.setLong(index, Long.class.cast(param));
        } else if (param instanceof Boolean) {
            stmt.setBoolean(index, Boolean.class.cast(param));
        } else if (param instanceof Double) {
            stmt.setDouble(index, Double.class.cast(param));
        } else if (param instanceof Float) {
            stmt.setFloat(index, Float.class.cast(param));
        } else if (param instanceof Date) {
            stmt.setDate(index, Date.class.cast(param));
        } else {
            stmt.setString(index, param.toString());
        }
    }
    
    public static void main(String[] args) {
        if (!getInfo(args)) return;
        
        ResultSet rs = null;
        PreparedStatement stmt = null;
        Connection conn = null;
        PrintWriter writer = null;
        try {
            Class.forName(dbClass);
            conn = DriverManager.getConnection(dbUrl, user, password);
            System.out.println("connected");
            conn.setAutoCommit(false);
            
            writer = new PrintWriter(output, outputCharset);

            stmt = conn.prepareStatement(sql);
            for (int i = 0; i < params.size();) {
                Object param = params.get(i);
                i += 1;
                setParam(stmt, i, param);
            }
            rs = stmt.executeQuery();
            System.out.println("Resultset fetched");
            
            ResultSetMetaData metaData = rs.getMetaData();
            int columns = metaData.getColumnCount();
            if (showHeader) {
                StringBuilder buffer = new StringBuilder();
                for (int i = 1; i <= columns; i += 1) {
                    buffer.append(splitText);
                    buffer.append(metaData.getColumnLabel(i));
                }
                writer.append(buffer.substring(splitLength));
                writer.append(lineBreak);
            }
            while (rs.next()) {
                StringBuilder buffer = new StringBuilder();
                for (int i = 1; i <= columns; i += 1) {
                    buffer.append(splitText);
                    String temp = rs.getString(i);
                    if (null == temp) {
                        buffer.append(nullStr);
                    } else {
                        buffer.append(temp);
                    }
                }
                writer.append(buffer.substring(splitLength));
                writer.append(lineBreak);
            }
            conn.rollback();
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            writer = safelyClose(writer);
            rs = safelyClose(rs);
            stmt = safelyClose(stmt);
            conn = safelyClose(conn);
        }
    }
    
    private static PrintWriter safelyClose(PrintWriter writer) {
        try {  
            if (null != writer) {  
                writer.close();
            }
        } catch (Exception ex) {
            ex.printStackTrace();  
        }
        return null;
    }
    private static ResultSet safelyClose(ResultSet rs) {
        try {  
            if (null != rs) {  
                rs.close();
            }
        } catch (Exception ex) {
            ex.printStackTrace();  
        }
        return null;
    }
    private static PreparedStatement safelyClose(PreparedStatement stmt) {
        try {  
            if (null != stmt) {  
                stmt.close();
            }
        } catch (Exception ex) {
            ex.printStackTrace();  
        }
        return null;
    }
    private static Connection safelyClose(Connection conn) {
        try {  
            if (null != conn) {  
                conn.close();
            }
        } catch (Exception ex) {
            ex.printStackTrace();  
        }
        return null;
    }
}
