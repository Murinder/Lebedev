package org.example.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple6;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

public class TwitterAnalysis implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final String TWEET_TIME_FORMAT = "yyyy-MM-dd HH:mm";
    private static final double MAX_REALISTIC_TWEETS_PER_HOUR = 1000.0;

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("TwitterAnalysis")
                .master("local[*]")
                .config("spark.driver.memory", "4g")
                .config("spark.executor.memory", "4g")
                .getOrCreate();

        //Загрузка данных из HDFS
        Dataset<Row> tweetsDF = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("hdfs://localhost:9000/user/twitter_data/ira_tweets_csv_hashed.csv");

        System.out.println("Общее количество твитов: " + tweetsDF.count());

        //Параметры для анализа
        int n = 50; //50% твитов
        int m = 5;  //минимум 5 ответов

        findUserWithNPercentTweetsHavingMRepliesSQL(spark, tweetsDF, n, m);

        findUserWithNPercentTweetsHavingMRepliesRDD(spark, tweetsDF, n, m);

        findUserWithHighestTweetingSpeedSQL(spark, tweetsDF);

        findUserWithHighestTweetingSpeedRDD(spark, tweetsDF);


        spark.stop();
    }

    public static void findUserWithNPercentTweetsHavingMRepliesSQL(SparkSession spark, Dataset<Row> df, int n, int m) {
        System.out.println("\n===== SparkSQL: Поиск пользователя, у которого >= " + n + "% твитов имеют минимум " + m + " ответов =====");

        //Регистрация DataFrame как временной таблицы
        df.createOrReplaceTempView("tweets");

        //SQL запрос для поиска пользователя
        String sqlQuery = "SELECT " +
                "    userid, " +
                "    COUNT(*) as total_tweets, " +
                "    SUM(CASE WHEN reply_count >= " + m + " THEN 1 ELSE 0 END) as tweets_with_m_replies, " +
                "    (SUM(CASE WHEN reply_count >= " + m + " THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as percent_tweets_with_m_replies " +
                "FROM tweets " +
                "WHERE reply_count IS NOT NULL AND reply_count != '' AND reply_count != ' ' " +
                "GROUP BY userid " +
                "HAVING COUNT(*) >= 10 AND percent_tweets_with_m_replies >= " + n + " " + // Минимум 10 твитов для значимости
                "ORDER BY percent_tweets_with_m_replies DESC, total_tweets DESC " +
                "LIMIT 10";

        Dataset<Row> result = spark.sql(sqlQuery);

        System.out.println("Топ пользователей с условием >= " + n + "% твитов с минимум " + m + " ответами:");
        result.show();
    }

    public static void findUserWithNPercentTweetsHavingMRepliesRDD(SparkSession spark, Dataset<Row> df, int n, int m) {
        System.out.println("\n===== RDD: Поиск пользователя, у которого >= " + n + "% твитов имеют минимум " + m + " ответов =====");

        //Фильтрация и выборка необходимых полей
        JavaRDD<Row> filteredTweets = df.select("userid", "reply_count")
                .filter(row -> {
                    String replyCountStr = row.getAs("reply_count");
                    return replyCountStr != null && !replyCountStr.trim().isEmpty() && !replyCountStr.equals(" ");
                })
                .javaRDD();

        //Преобразование reply_count в целое число и группировка по пользователю
        JavaPairRDD<String, Tuple2<Integer, Integer>> userStats = filteredTweets
                .mapToPair(new PairFunction<Row, String, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<String, Tuple2<Integer, Integer>> call(Row row) throws Exception {
                        String userId = row.getAs("userid");
                        String replyCountStr = row.getAs("reply_count");

                        int replyCount;
                        try {
                            replyCount = Integer.parseInt(replyCountStr.trim());
                        } catch (NumberFormatException e) {
                            replyCount = 0;
                        }

                        //Если ответов >= m, то считаем 1 для числителя, иначе 0
                        //Всегда считаем 1 для знаменателя
                        return new Tuple2<>(userId, new Tuple2<>(replyCount >= m ? 1 : 0, 1));
                    }
                })
                .reduceByKey(new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> v1, Tuple2<Integer, Integer> v2) throws Exception {
                        return new Tuple2<>(v1._1() + v2._1(), v1._2() + v2._2());
                    }
                });

        //Вычисление процента и фильтрация
        JavaRDD<Tuple4<String, Integer, Integer, Double>> userPercentages = userStats
                .map(new Function<Tuple2<String, Tuple2<Integer, Integer>>, Tuple4<String, Integer, Integer, Double>>() {
                    @Override
                    public Tuple4<String, Integer, Integer, Double> call(Tuple2<String, Tuple2<Integer, Integer>> tuple) throws Exception {
                        String userId = tuple._1();
                        int tweetsWithMReplies = tuple._2()._1();
                        int totalTweets = tuple._2()._2();
                        double percent = (totalTweets > 0) ? (tweetsWithMReplies * 100.0) / totalTweets : 0.0;

                        return new Tuple4<>(userId, tweetsWithMReplies, totalTweets, percent);
                    }
                })
                .filter(tuple -> tuple._3() >= 10 && tuple._4() >= n); // Минимум 10 твитов и процент >= n

        //Сортировка и выборка топ-10 пользователей
        List<Tuple4<String, Integer, Integer, Double>> topUsers = userPercentages
                .sortBy(new Function<Tuple4<String, Integer, Integer, Double>, Double>() {
                    @Override
                    public Double call(Tuple4<String, Integer, Integer, Double> tuple) throws Exception {
                        return tuple._4(); // Сортировка по проценту
                    }
                }, false, 10) // false - по убыванию
                .collect();

        System.out.println("Топ пользователей (RDD подход):");
        System.out.println("UserID\tТвитов с " + m + "+ ответами\tВсего твитов\tПроцент");
        for (Tuple4<String, Integer, Integer, Double> user : topUsers) {
            System.out.printf("%s\t%d\t%d\t%.2f%%\n",
                    user._1(), user._2(), user._3(), user._4());
        }
    }

    public static void findUserWithHighestTweetingSpeedSQL(SparkSession spark, Dataset<Row> df) {
        System.out.println("\n===== SparkSQL: Поиск пользователя с наибольшей скоростью написания сообщений =====");

        //Регистрация DataFrame как временной таблицы
        df.createOrReplaceTempView("tweets");

        //SQL запрос для расчета скорости написания сообщений
        String sqlQuery = "WITH valid_tweets AS (" +
                "    SELECT " +
                "        userid, " +
                "        tweet_time, " +
                "        TO_TIMESTAMP(tweet_time, '" + TWEET_TIME_FORMAT + "') as parsed_time " +
                "    FROM tweets " +
                "    WHERE tweet_time IS NOT NULL " +
                "        AND tweet_time != '' " +
                "        AND tweet_time != ' '" +
                "        AND LENGTH(tweet_time) >= 16 " + //Минимальная длина для формата "yyyy-MM-dd HH:mm"
                "), user_tweet_stats AS (" +
                "    SELECT " +
                "        userid, " +
                "        COUNT(*) as total_tweets, " +
                "        MIN(parsed_time) as first_tweet_time, " +
                "        MAX(parsed_time) as last_tweet_time, " +
                "        (UNIX_TIMESTAMP(MAX(parsed_time)) - UNIX_TIMESTAMP(MIN(parsed_time))) as time_diff_seconds " +
                "    FROM valid_tweets " +
                "    WHERE parsed_time IS NOT NULL " +
                "    GROUP BY userid " +
                "    HAVING total_tweets >= 10 " +
                "        AND time_diff_seconds > 0 " +
                "        AND time_diff_seconds <= 31536000" + //Не больше года (в секундах)
                "), user_speed_stats AS (" +
                "    SELECT " +
                "        userid, " +
                "        total_tweets, " +
                "        first_tweet_time, " +
                "        last_tweet_time, " +
                "        time_diff_seconds, " +
                "        total_tweets * 3600.0 / time_diff_seconds as tweets_per_hour " +
                "    FROM user_tweet_stats " +
                "    WHERE total_tweets * 3600.0 / time_diff_seconds <= " + MAX_REALISTIC_TWEETS_PER_HOUR + // Ограничение реалистичной скорости
                ")" +
                "SELECT " +
                "    userid, " +
                "    total_tweets, " +
                "    first_tweet_time, " +
                "    last_tweet_time, " +
                "    time_diff_seconds, " +
                "    tweets_per_hour " +
                "FROM user_speed_stats " +
                "ORDER BY tweets_per_hour DESC " +
                "LIMIT 10";

        Dataset<Row> result = spark.sql(sqlQuery);

        System.out.println("Топ пользователей по скорости написания сообщений (твитов в час):");
        result.show();
    }

    public static void findUserWithHighestTweetingSpeedRDD(SparkSession spark, Dataset<Row> df) {
        System.out.println("\n===== RDD: Поиск пользователя с наибольшей скоростью написания сообщений =====");

        //Фильтрация и выборка необходимых полей
        JavaRDD<Row> filteredTweets = df.select("userid", "tweet_time")
                .filter(row -> {
                    String tweetTime = row.getAs("tweet_time");
                    return tweetTime != null &&
                            !tweetTime.trim().isEmpty() &&
                            !tweetTime.equals(" ") &&
                            tweetTime.trim().length() >= 16; //Минимальная длина для формата
                })
                .javaRDD();

        //Создаем форматтер времени с UTC временной зоной для консистентности
        SimpleDateFormat format = new SimpleDateFormat(TWEET_TIME_FORMAT);
        format.setTimeZone(TimeZone.getTimeZone("UTC"));

        //Группировка твитов по пользователю и нахождение минимального и максимального времени
        JavaPairRDD<String, Tuple3<Long, Long, Integer>> userTimeStats = filteredTweets
                .mapToPair(new PairFunction<Row, String, Tuple3<Long, Long, Integer>>() {
                    @Override
                    public Tuple2<String, Tuple3<Long, Long, Integer>> call(Row row) throws Exception {
                        String userId = row.getAs("userid");
                        String tweetTimeStr = row.getString(row.fieldIndex("tweet_time")).trim();
                        try {
                            //Явно указываем временные зоны для консистентности
                            Date date = format.parse(tweetTimeStr);
                            long timestamp = date.getTime() / 1000; //в секунды

                            //Возвращаем пару (мин_время, макс_время, счетчик) для каждого твита
                            return new Tuple2<>(userId, new Tuple3<>(timestamp, timestamp, 1));
                        } catch (ParseException e) {
                            //Пропускаем некорректные временные метки вместо использования текущего времени
                            return new Tuple2<>(userId, new Tuple3<>(Long.MAX_VALUE, Long.MIN_VALUE, 0));
                        }
                    }
                })
                .reduceByKey(new Function2<Tuple3<Long, Long, Integer>, Tuple3<Long, Long, Integer>, Tuple3<Long, Long, Integer>>() {
                    @Override
                    public Tuple3<Long, Long, Integer> call(Tuple3<Long, Long, Integer> v1, Tuple3<Long, Long, Integer> v2) throws Exception {
                        //Обработка некорректных значений (Long.MAX_VALUE, Long.MIN_VALUE)
                        long min1 = v1._1() == Long.MAX_VALUE ? v2._1() : v1._1();
                        long min2 = v2._1() == Long.MAX_VALUE ? v1._1() : v2._1();
                        long max1 = v1._2() == Long.MIN_VALUE ? v2._2() : v1._2();
                        long max2 = v2._2() == Long.MIN_VALUE ? v1._2() : v2._2();

                        long minTime = Math.min(min1, min2);
                        long maxTime = Math.max(max1, max2);
                        int count = v1._3() + v2._3();

                        return new Tuple3<>(minTime, maxTime, count);
                    }
                })
                //Фильтрация некорректных данных и данных с нереалистичными промежутками
                .filter(tuple -> {
                    long minTime = tuple._2()._1();
                    long maxTime = tuple._2()._2();
                    int count = tuple._2()._3();
                    long timeDiff = maxTime - minTime;

                    return count >= 10 && // Минимум 10 твитов
                            timeDiff > 0 && // Разница во времени > 0
                            timeDiff <= 31536000 && // Не больше года в секундах
                            minTime != Long.MAX_VALUE && // Фильтрация некорректных значений
                            maxTime != Long.MIN_VALUE;
                });

        //Расчет скорости
        JavaRDD<Tuple6<String, Integer, String, String, Long, Double>> userSpeeds = userTimeStats
                .map(new Function<Tuple2<String, Tuple3<Long, Long, Integer>>, Tuple6<String, Integer, String, String, Long, Double>>() {
                    @Override
                    public Tuple6<String, Integer, String, String, Long, Double> call(Tuple2<String, Tuple3<Long, Long, Integer>> tuple) throws Exception {
                        String userId = tuple._1();
                        long minTime = tuple._2()._1();
                        long maxTime = tuple._2()._2();
                        int tweetCount = tuple._2()._3();
                        long timeDiff = maxTime - minTime;

                        // Форматирование времени для вывода
                        String firstTweetTime = format.format(new Date(minTime * 1000));
                        String lastTweetTime = format.format(new Date(maxTime * 1000));

                        double tweetsPerHour = tweetCount * 3600.0 / timeDiff;

                        // Дополнительная фильтрация для удаления аномальных значений
                        if (tweetsPerHour > MAX_REALISTIC_TWEETS_PER_HOUR) {
                            tweetsPerHour = 0; // Будет отфильтровано ниже
                        }

                        return new Tuple6<>(userId, tweetCount, firstTweetTime, lastTweetTime, timeDiff, tweetsPerHour);
                    }
                })
                .filter(tuple -> tuple._6() > 0 && tuple._6() <= MAX_REALISTIC_TWEETS_PER_HOUR);

        //Сортировка и выборка топ-10 пользователей
        List<Tuple6<String, Integer, String, String, Long, Double>> topUsers = userSpeeds
                .sortBy(Tuple6::_6, false, userSpeeds.getNumPartitions())
                .take(10); // ⬅️ Берём только первые 10

        System.out.println("Топ пользователей по скорости написания сообщений (RDD подход):");
        System.out.println("UserID\tВсего твитов\tПервый твит\tПоследний твит\tРазница (сек)\tСкорость (твитов/час)");
        for (Tuple6<String, Integer, String, String, Long, Double> user : topUsers) {
            System.out.printf("%s\t%d\t%s\t%s\t%d\t%.2f\n",
                    user._1(), user._2(), user._3(), user._4(), user._5(), user._6());
        }
    }
}