import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class Main {
    private static final String SEPARATOR = ",";

    private static String toStringInBracketsAndQuotes(CSVRecord record){
        StringBuilder result = new StringBuilder("(");
        boolean first = true;
        for (String s : record) {
            if (!first) result.append(",");
            first = false;
            result.append("'").append(s).append("'");
        }
        return result.append(")").toString();
    }

    public static void main(String[] args) throws IOException, SQLException {
        BufferedReader bufferedReader = new BufferedReader(new FileReader(args[0]));
        CSVParser parser = CSVFormat.Builder.create(CSVFormat.DEFAULT)
                .setRecordSeparator(SEPARATOR).build().parse(bufferedReader);
        List<CSVRecord> records = parser.getRecords();
        if (records.size() < 1) throw new IllegalArgumentException("empty csv");
        CSVRecord headers = records.get(0);


        try (Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:aname", "sa", "")) {

            StringBuilder statement = new StringBuilder("CREATE TABLE PUBLIC.TEST(");
            boolean first = true;

            for (String header : headers) {
                if (!first) {
                    statement.append(",\n");
                }
                first = false;
                statement.append(header).append(" ").append("varchar(255)");
            }
            statement.append(");");
            connection.createStatement().executeUpdate(statement.toString());
            connection.commit();

            first = true;
            for (CSVRecord record : records) {
                if (!first) {
                    connection.createStatement().executeUpdate("insert into PUBLIC.TEST values "
                            + toStringInBracketsAndQuotes(record));
                }
                first = false;
            }
            connection.commit();

            ResultSet resultSet = connection.createStatement().executeQuery("select City,Country,Latitude,Longitude from PUBLIC.TEST;");
            resultSet.next();
            System.out.println(resultSet.getString(1));
            System.out.println(resultSet.getString(2));
            System.out.println(resultSet.getString(3));
            System.out.println(resultSet.getString(4));
            resultSet.next();
            System.out.println(resultSet.getString(1));
            System.out.println(resultSet.getString(2));
            System.out.println(resultSet.getString(3));
            System.out.println(resultSet.getString(4));
        }
    }
}
