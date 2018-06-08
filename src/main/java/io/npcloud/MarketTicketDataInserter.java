/**
 * author:chengyezhao
 * email:chengyezhao@gmail.com
 */


package io.npcloud;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/***
 * csv data format:
 *
 * 市场代码,证券代码,时间,最新价,成交笔数,成交额,成交量,方向,买一价,买二价,买三价,买四价,买五价,卖一价,卖二价,卖三价,卖四价,卖五价,买一量,买二量,买三量,买四量,买五量,卖一量,卖二量,卖三量,卖四量,卖五量
 * sh,000003,2016-04-01 09:25:06,374.7400,0,472801.0000,632,B,0.0000,0.0000,0.0000,0.0000,0.0000,0.0000,0.0000,0.0000,0.0000,0.0000,0,0,0,0,0,0,0,0,0,0
 *
 * insert data into a database table "ticks"
 *
 */
public class MarketTicketDataInserter {

    public static void main(String[] args){
        String dataPath = args[0];
        String databaseHost = args[1];
        String databaseName = args[2];
        String dbUser = args[3];
        String dbPasswd = args[4];
        // 指定需要导入的年和月
        String year = args[5];
        String month = args[6];

        //scan directory to get file name for each day
        System.out.println("Collecting market data file info!");
        Map<String, List<String>> filesInEachDay = new ConcurrentSkipListMap<>();
        String[] list = new File(dataPath).list();
        String yearPath = dataPath + File.separator + year;
        String monthPath = yearPath +File.separator +month;
        for(String day: new File(monthPath).list()){
            String dayPath = monthPath +File.separator + day;
            List<String> files = new ArrayList<>();
            for(String symbol : new File(dayPath).list()){
                files.add(dayPath + File.separator + symbol);
            }
            filesInEachDay.put(day, files);
        }
        System.out.println("Collection complete!");

        //connect database
        DBUtil dbUtil = new DBUtil();
        dbUtil.setJdbcUrl("jdbc:postgresql://" + databaseHost + ":5432/" + databaseName);
        dbUtil.setJdbcDriverClassName("org.postgresql.Driver");
        dbUtil.setJdbcUsername(dbUser);
        dbUtil.setJdbcPassword(dbPasswd);

        try {
            Connection connection = dbUtil.getConnection();
            connection.setAutoCommit(false);
            String sql = "INSERT INTO \"public\".\"ticks\" (market,symbol,trade_time,latest_price,trade_num,trade_amount,trade_volume," +
                    "BS, B1P, B2P, B3P, B4P, B5P, S1P, S2P, S3P, S4P, S5P, B1V," +
                    "B2V, B3V, B4V, B5V, S1V, S2V, S3V, S4V, S5V) values (?,?,?,?,?,?,?, ?, ?, ?, " +
                    "?,?,?,?,?,?,?, ?, ?, ?, ?,?,?,?,?,?,?, ?)";

            final int splitNum = 20;
            Executor executor = Executors.newFixedThreadPool(5);

            for(String day: filesInEachDay.keySet()){
                List<String> dayFiles = filesInEachDay.get(day);
                final AtomicLong totalInsertMS = new AtomicLong(0);
                final AtomicLong totalInsertCount = new AtomicLong(0);

                //read data in parallel
                final CountDownLatch countDownLatch = new CountDownLatch(splitNum);
                final PreparedStatement[] pss = new PreparedStatement[splitNum];
                for(int partition = 0; partition < splitNum; partition++) {
                    final int partNum = partition;
                    executor.execute(new Runnable() {
                        @Override
                        public void run() {
                            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                            try {
                                pss[partNum] = connection.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE,
                                        ResultSet.CONCUR_READ_ONLY);
                                int index = 0;
                                for (String filename : dayFiles) {
                                    if (index % splitNum == partNum) {
//                                        System.out.println(partNum + "--->" + filename);
                                        File file = new File(filename);
                                        BufferedReader reader = new BufferedReader(new FileReader(file));
                                        //skip first line
                                        reader.readLine();
                                        while (true) {
                                            String line = reader.readLine();
                                            if (line == null) {
                                                break;
                                            }
                                            String[] columns = line.split(",");
                                            pss[partNum].setInt(1, columns[0].equals("sh") ? 0 : 1);
                                            pss[partNum].setString(2, columns[1]);
                                            pss[partNum].setTimestamp(3, new Timestamp(df.parse(columns[2]).getTime()));
                                            pss[partNum].setDouble(4, Double.valueOf(columns[3]));
                                            pss[partNum].setDouble(5, Double.valueOf(columns[4]));
                                            pss[partNum].setDouble(6, Double.valueOf(columns[5]));
                                            pss[partNum].setDouble(7, Double.valueOf(columns[6]));
                                            pss[partNum].setInt(8, columns[7].equals("B") ? 0 : 1);
                                            for (int i = 9; i < 29; i++) {
                                                pss[partNum].setDouble(i, Double.valueOf(columns[i - 1]));

                                            }
                                            totalInsertCount.incrementAndGet();
                                            pss[partNum].addBatch();
                                        }
                                    }
                                    index++;
                                }

                            } catch (Exception e) {
                                System.out.println("Failed to insert record" + e);
                            }finally {
                                countDownLatch.countDown();
                            }

                        }
                    });

                }
                countDownLatch.await();
                for(int i = 0; i < splitNum; i++){
                    // save to db
                    //start timestamp
                    System.out.println("part " + i + " execute batch insert start");
                    long startTime = System.currentTimeMillis();
                    // execute batch and commit
                    int[] count = pss[i].executeBatch();
                    connection.commit();
                    totalInsertMS.addAndGet(System.currentTimeMillis() - startTime);
                    pss[i].close();
                    System.out.println("part " + i + " execute batch insert finish");
                }
                long insertRate = totalInsertCount.get() * 1000L / totalInsertMS.get();
                System.out.println("Total speed #######" + day + "\t" + totalInsertCount + "\t" + insertRate);
            }

            connection.close();
        }catch (Exception e){
            System.out.println("Failed to insert record" + e);
        }
    }

}
