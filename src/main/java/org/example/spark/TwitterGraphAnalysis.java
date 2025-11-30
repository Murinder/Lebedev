package org.example.spark;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.graphframes.GraphFrame;
import org.graphframes.lib.BFS;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.math.BigDecimal;
import java.util.*;

import static org.apache.spark.sql.functions.*;

public class TwitterGraphAnalysis {

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: java TwitterGraphAnalysis <task>");
            System.err.println("Available tasks: task1, task2, task3");
            System.exit(1);
        }

        String task = args[0].toLowerCase();
        switch (task) {
            case "task1":
                runTask1_ChainStarter(2);
                break;
            case "task2":
                runTask2_LargestComponents();
                break;
            case "task3":
                runTask3_PoliticalGroups(0.40);
                break;
            default:
                System.err.println("Unknown task: " + task + ". Use task1, task2, or task3.");
                System.exit(1);
        }
    }

    public static void runTask1_ChainStarter(int nChain) {
        SparkSession spark = createSparkSession();
        try {
            Dataset<Row> sampledDF = loadAndSampleData(spark);
            findNthLongestChainStarter(spark, sampledDF, nChain);
        } finally {
            spark.stop();
        }
    }

    public static void runTask2_LargestComponents() {
        SparkSession spark = createSparkSession();
        try {
            Dataset<Row> sampledDF = loadAndSampleData(spark);

            Column russianFilter = col("user_reported_location").isNotNull()
                    .and(col("user_reported_location").like("%Россия%")
                            .or(col("user_reported_location").like("%Russia%"))
                            .or(col("user_reported_location").like("%РФ%")));

            Column moscowFilter = col("user_reported_location").isNotNull()
                    .and(col("user_reported_location").like("%Москва%")
                            .or(col("user_reported_location").like("%Moscow%"))
                            .or(col("user_reported_location").like("%Msk%")));

            Column foreignFilter = col("user_reported_location").isNotNull()
                    .and(not(col("user_reported_location").like("%Россия%")))
                    .and(not(col("user_reported_location").like("%Russia%")))
                    .and(not(col("user_reported_location").like("%РФ%")))
                    .and(not(col("user_reported_location").like("%Москва%")))
                    .and(not(col("user_reported_location").like("%Moscow%")))
                    .and(not(col("user_reported_location").like("%Msk%")));

            findLargestComponentForGroup(spark, sampledDF, russianFilter, "RUSSIAN");
            findLargestComponentForGroup(spark, sampledDF, moscowFilter, "MOSCOW");
            findLargestComponentForGroup(spark, sampledDF, foreignFilter, "FOREIGN");

        } finally {
            spark.stop();
        }
    }

    public static void runTask3_PoliticalGroups(double threshold) {
        SparkSession spark = createSparkSession();
        try {
            Dataset<Row> sampledDF = loadAndSampleData(spark);
            findPoliticalConversationGroups(spark, sampledDF, threshold);
        } finally {
            spark.stop();
        }
    }

    private static SparkSession createSparkSession() {
        return SparkSession.builder()
                .appName("TwitterGraphAnalysis")
                .master("local[*]")
                .config("spark.driver.memory", "8g")
                .config("spark.executor.memory", "8g")
                .config("spark.memory.fraction", "0.8")
                .config("spark.memory.storageFraction", "0.3")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.sql.shuffle.partitions", "200")
                .config("spark.default.parallelism", "200")
                .getOrCreate();
    }

    private static Dataset<Row> loadAndSampleData(SparkSession spark) {
        spark.sparkContext().setCheckpointDir("/tmp");

        Dataset<Row> tweetsDF = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .option("multiLine", true)
                .option("escape", "\"")
                .csv("hdfs://localhost:9000/user/twitter_data/ira_tweets_csv_hashed.csv");

        System.out.println("Общее количество твитов: " + tweetsDF.count());

        Dataset<Row> sampledDF = tweetsDF.sample(false, 0.5, 42);
        System.out.println("Сэмпл (1%): " + sampledDF.count());

        return sampledDF;
    }

    /**
     * Найти пользователя, начавшего n-ю по количеству сообщений непрерывную цепочку
     */
    public static void findNthLongestChainStarter(SparkSession spark, Dataset<Row> tweetsDF, int nChain) {
        System.out.println("\n===== GraphFrames: Поиск пользователя с " + nChain + "-й по длине цепочкой сообщений =====");

        // Создание ребер для цепочек сообщений (ответы)
        Dataset<Row> edges = tweetsDF.filter(
                        col("userid").isNotNull()
                                .and(col("in_reply_to_userid").isNotNull())
                                .and(col("in_reply_to_userid").notEqual(""))
                                .and(col("in_reply_to_userid").notEqual("null")))
                .select(
                        col("in_reply_to_userid").alias("src"),  // Автор оригинального сообщения
                        col("userid").alias("dst"),               // Кто ответил
                        col("tweetid").alias("edge_id")
                )
                .distinct();

        System.out.println("Количество ребер для цепочек: " + edges.count());

        // Создание вершин (пользователи)
        Dataset<Row> vertices = tweetsDF.select(
                        col("userid").alias("id"),
                        col("user_display_name").alias("name"))
                .distinct()
                .filter(col("id").isNotNull());

        System.out.println("Количество вершин: " + vertices.count());

        // Создание графа
        GraphFrame graph = GraphFrame.apply(vertices, edges);

        // Вычисление степеней вершин
        Dataset<Row> outDegrees = graph.outDegrees().withColumnRenamed("outDegree", "out");
        Dataset<Row> inDegrees = graph.inDegrees().withColumnRenamed("inDegree", "in");

// Потенциальные стартовые пользователи: те, кто имеет исходящие связи, но не имеет входящих
        Column joinCondition1 = vertices.col("id").equalTo(outDegrees.col("id"));
        Column joinCondition2 = vertices.col("id").equalTo(inDegrees.col("id"));

        Dataset<Row> potentialStarters = vertices
                .join(outDegrees, joinCondition1, "left")
                .join(inDegrees, joinCondition2, "left")
                .na().fill(0, new String[]{"out", "in"})
                .filter(col("out").gt(0));
        System.out.println("Количество потенциальных стартовых пользователей: " + potentialStarters.count());

        // Получение топ-N пользователей с наибольшей исходящей степенью для анализа
        int topN = 100;
        List<Row> topStarters = potentialStarters.orderBy(desc("out")).limit(topN).collectAsList();

        System.out.println("Анализ цепочек для " + topStarters.size() + " пользователей...");

        // Хранение информации о длинах цепочек
        List<Map<String, Object>> chainLengths = new ArrayList<>();

        for (Row starter : topStarters) {
            String starterId = starter.getAs("id");
            String starterName = starter.getAs("name");
            long outDegree = ((Number) starter.getAs("out")).longValue();
            long inDegree = ((Number) starter.getAs("in")).longValue();

            try {
                // BFS для поиска цепочки от этого пользователя
                Column fromExpr = col("id").equalTo(starterId);
                Column toExpr = col("outDegree").isNull().or(col("outDegree").equalTo(0));

                // Поиск путей с максимальной длиной 10
                BFS bfs = graph.bfs()
                        .fromExpr(fromExpr)
                        .toExpr(toExpr)
                        .maxPathLength(10);

                Dataset<Row> paths = bfs.run();
                long pathCount = paths.count();

                if (pathCount > 0) {
                    Map<String, Object> chainInfo = new HashMap<>();
                    chainInfo.put("user_id", starterId);
                    chainInfo.put("user_name", starterName);
                    chainInfo.put("chain_length", pathCount);
                    chainInfo.put("out_degree", outDegree);
                    chainInfo.put("in_degree", inDegree);
                    chainLengths.add(chainInfo);
                }
            } catch (Exception e) {
                // В случае ошибки используем исходящую степень как приближение
                Map<String, Object> chainInfo = new HashMap<>();
                chainInfo.put("user_id", starterId);
                chainInfo.put("user_name", starterName);
                chainInfo.put("chain_length", outDegree);
                chainInfo.put("out_degree", outDegree);
                chainInfo.put("in_degree", inDegree);
                chainLengths.add(chainInfo);
            }
        }

        // Сортировка цепочек по длине в убывающем порядке
        chainLengths.sort((a, b) -> Long.compare(
                ((Number) b.get("chain_length")).longValue(),
                ((Number) a.get("chain_length")).longValue()
        ));

        // Вывод топ-N цепочек
        int displayCount = Math.min(nChain * 2, chainLengths.size());
        System.out.println("\nТоп " + displayCount + " самых длинных цепочек:");
        System.out.println(String.join("", Collections.nCopies(80, "=")));
        for (int i = 0; i < displayCount; i++) {
            Map<String, Object> chain = chainLengths.get(i);
            String userName = chain.get("user_name").toString();
            if (userName.length() > 30) {
                userName = userName.substring(0, 27) + "...";
            }

            System.out.printf("%d. %-30s | Цепочка: %4d msg | Out: %3d | In: %3d%n",
                    i + 1,
                    userName,
                    chain.get("chain_length"),
                    chain.get("out_degree"),
                    chain.get("in_degree"));
        }

        // N-й по длине цепочки
        if (chainLengths.size() >= nChain) {
            Map<String, Object> nthChain = chainLengths.get(nChain - 1);
            System.out.println("\n" + String.join("", Collections.nCopies(80, "=")));
            System.out.println(nChain + "-й пользователь с самой длинной цепочкой:");
            System.out.println(String.join("", Collections.nCopies(80, "=")));
            System.out.println("Пользователь: " + nthChain.get("user_name"));
            System.out.println("ID пользователя: " + nthChain.get("user_id"));
            System.out.println("Длина цепочки: " + nthChain.get("chain_length") + " сообщений");
            System.out.println("Исходящая степень: " + nthChain.get("out_degree"));
            System.out.println("Входящая степень: " + nthChain.get("in_degree"));
            System.out.println(String.join("", Collections.nCopies(80, "=")));
        } else {
            System.out.println("\nНайдено только " + chainLengths.size() + " цепочек, недостаточно для определения " + nChain + "-й");
        }
    }

    /**
     * Найти наибольшую компоненту связности для заданной группы пользователей
     */
    public static void findLargestComponentForGroup(SparkSession spark, Dataset<Row> tweetsDF, Column locationFilter, String groupName) {
        System.out.println("\n" + String.join("", Collections.nCopies(80, "=")));
        System.out.println("Анализ группы: " + groupName);
        System.out.println(String.join("", Collections.nCopies(80, "=")));

        // Фильтрация твитов по локации
        Dataset<Row> filteredTweets = tweetsDF.filter(locationFilter);
        System.out.println("Количество твитов в группе " + groupName + ": " + filteredTweets.count());

        if (filteredTweets.count() == 0) {
            System.out.println("Не найдено твитов для группы " + groupName);
            return;
        }

        // Создание вершин (пользователи)
        Dataset<Row> vertices = filteredTweets.select(
                        col("userid").alias("id"),
                        col("user_display_name").alias("name"),
                        col("user_reported_location").alias("location"))
                .distinct()
                .filter(col("id").isNotNull());

        System.out.println("Количество пользователей в группе " + groupName + ": " + vertices.count());

        if (vertices.count() == 0) {
            System.out.println("Не найдено пользователей в группе " + groupName);
            return;
        }

        // Создание ребер для ответов
        Dataset<Row> replyEdges = filteredTweets
                .filter(col("in_reply_to_userid").isNotNull()
                        .and(col("in_reply_to_userid").notEqual(""))
                        .and(col("in_reply_to_userid").notEqual("null")))
                .select(
                        col("userid").alias("src"),
                        col("in_reply_to_userid").alias("dst")
                );

        // Объединение всех ребер
        Dataset<Row> edges = replyEdges
                .filter(col("src").isNotNull().and(col("src").notEqual("")))
                .filter(col("dst").isNotNull().and(col("dst").notEqual("")))
                .filter(col("src").notEqual(col("dst"))) // Исключение петель
                .distinct();

        System.out.println("Количество ребер для группы " + groupName + ": " + edges.count());

        if (edges.count() == 0) {
            System.out.println("Не найдено связей между пользователями в группе " + groupName);
            return;
        }

        // Проверка наличия вершин для всех ребер
        Dataset<Row> srcVertices = edges.select(col("src").alias("id")).distinct();
        Dataset<Row> dstVertices = edges.select(col("dst").alias("id")).distinct();
        Dataset<Row> allEdgeVertices = srcVertices.union(dstVertices).distinct();

        // Фильтрация вершин, оставляя только те, у которых есть ребра
        vertices = vertices.join(allEdgeVertices, "id");

        long verticesWithEdges = vertices.count();
        System.out.println("Количество вершин с ребрами в группе " + groupName + ": " + verticesWithEdges);

        if (verticesWithEdges == 0) {
            System.out.println("Недостаточно данных для построения графа в группе " + groupName);
            return;
        }

        // Создание графа
        GraphFrame graph = GraphFrame.apply(vertices, edges);

        // Поиск компонент связности
        try {
            Dataset<Row> components = graph.connectedComponents().setAlgorithm("graphframes").run();

            // Группировка по компонентам и подсчет размера
            Dataset<Row> componentSizes = components.groupBy("component")
                    .agg(
                            count("*").alias("size"),
                            collect_list("name").alias("users")
                    )
                    .orderBy(col("size").desc());

            if (componentSizes.count() == 0) {
                System.out.println("Не найдено компонент связности в группе " + groupName);
                return;
            }

            // Получение наибольшей компоненты
            Row largestComponent = componentSizes.first();
            long componentId = largestComponent.getAs("component");
            long componentSize = largestComponent.getAs("size");
            Seq<String> usersSeq = largestComponent.getAs("users");
            List<String> users = JavaConverters.seqAsJavaListConverter(usersSeq).asJava();

            System.out.printf("[РЕЗУЛЬТАТ] Наибольшая компонента связности в группе %s: ID=%d, размер=%d пользователей%n",
                    groupName, componentId, componentSize);

            // Вывод пользователей из компоненты
            System.out.println("\nКомпонента включает следующих пользователей:");
            int sampleSize = Math.min(10, users.size());
            for (int i = 0; i < sampleSize; i++) {
                System.out.printf("  %d. %s%n", i + 1, users.get(i));
            }

            if (users.size() > sampleSize) {
                System.out.printf("  ... и еще %d пользователей%n", users.size() - sampleSize);
            }
        } catch (Exception e) {
            System.out.println("Ошибка при поиске компонент связности в группе " + groupName + ": " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Найти группу пользователей, беседы которой на n% состоят из сообщений на политическую тему
     */
    public static void findPoliticalConversationGroups(SparkSession spark, Dataset<Row> tweetsDF, double politicalThreshold) {
        System.out.println("\n===== GraphFrames: Поиск групп пользователей с политическими беседами =====");

        // Список политических ключевых слов для анализа
        List<String> politicalKeywords = Arrays.asList("путин", "байден", "тр", "джо", "джо байден", "владимир", "владимир путин",
                "выборы", "президент", "голосование", "политика", "партия", "единая россия", "кр", "коммунист", "либерал",
                "демократ", "республиканец", "закон", "правительство", "госдума", "конгресс", "сенат", "депутат", "министр",
                "казначейство", "бюджет", "налог", "санкци", "америк", "росс", "украин", "войн", "мирг", "навальн", "оппозиц",
                "кремль", "белый дом", "демократи", "республикан", "выборов", "президентск", "парламент", "федерал", "губернатор",
                "мер", "премьер", "министерство", "дипломат", "посол", "избиратель", "митинг", "акция", "протест", "власть");

        // Оптимизированная UDF для определения политического контента
        Set<String> politicalKeywordsSet = new HashSet<>(politicalKeywords);
        UserDefinedFunction isPolitical = udf((String text) -> {
            if (text == null || text.isEmpty()) return false;

            String lowerText = text.toLowerCase();
            if (!lowerText.contains("путин") && !lowerText.contains("байден") && !lowerText.contains("выбор")) {
                return false;
            }

            String cleanedText = lowerText
                    .replaceAll("[^\\p{L}\\p{N}\\s]", " ")
                    .replaceAll("\\s+", " ")
                    .trim();

            StringTokenizer tokenizer = new StringTokenizer(cleanedText, " ");
            while (tokenizer.hasMoreTokens()) {
                String word = tokenizer.nextToken();
                if (word.length() >= 3 && politicalKeywordsSet.contains(word)) {
                    return true;
                }
                if (word.length() >= 4) {
                    for (String keyword : politicalKeywordsSet) {
                        if (keyword.length() <= 5 && word.contains(keyword)) {
                            return true;
                        }
                    }
                }
            }
            return false;
        }, DataTypes.BooleanType);

        spark.udf().register("is_political", isPolitical);

        // Фильтрация твитов и пометка политических
        System.out.println("Анализ политического контента в твитах...");
        Dataset<Row> allTweets = tweetsDF
                .filter(col("tweet_text").isNotNull())
                .filter(col("tweet_text").notEqual(""))
                .filter(col("userid").isNotNull())
                .select(
                        col("userid"),
                        col("user_display_name"),
                        col("user_reported_location"),
                        col("tweet_text"),
                        col("in_reply_to_userid"),
                        col("user_mentions")
                )
                .withColumn("is_political", isPolitical.apply(col("tweet_text")));

        System.out.println("Общее количество твитов для анализа: " + allTweets.count());

        long politicalTweetsCount = allTweets.filter(col("is_political").equalTo(true)).count();
        System.out.println("Количество твитов с политическим контентом: " + politicalTweetsCount);

        if (politicalTweetsCount == 0) {
            System.out.println("Не найдено твитов с политическим контентом");
            return;
        }

        // Подготовка вершин графа (пользователи)
        System.out.println("Подготовка вершин графа...");
        Dataset<Row> vertices = allTweets.select(
                        col("userid").alias("id"),
                        col("user_display_name").alias("name"),
                        col("user_reported_location").alias("location")
                )
                .distinct();

        System.out.println("Количество пользователей в графе: " + vertices.count());

        // Подготовка ребер графа (взаимодействия)
        System.out.println("Подготовка ребер графа...");
        Dataset<Row> replyEdges = allTweets
                .filter(col("in_reply_to_userid").isNotNull()
                        .and(col("in_reply_to_userid").notEqual(""))
                        .and(col("in_reply_to_userid").notEqual("null")))
                .select(
                        col("userid").alias("src"),
                        col("in_reply_to_userid").alias("dst"),
                        col("is_political")
                )
                .filter(col("src").notEqual(col("dst"))); // Исключение петель

        Dataset<Row> mentionEdges = allTweets
                .filter(col("user_mentions").isNotNull()
                        .and(col("user_mentions").notEqual("[]"))
                        .and(col("user_mentions").notEqual("null")))
                .select(
                        col("userid").alias("src"),
                        explode(from_json(col("user_mentions"), DataTypes.createArrayType(DataTypes.StringType))).alias("dst"),
                        col("is_political")
                )
                .filter(col("dst").isNotNull()
                        .and(col("dst").notEqual(""))
                        .and(col("dst").notEqual("null")))
                .filter(col("src").notEqual(col("dst"))); // Исключение петель

        // Объединение всех ребер
        Dataset<Row> edges = replyEdges.union(mentionEdges)
                .filter(col("src").isNotNull().and(col("src").notEqual("")))
                .filter(col("dst").isNotNull().and(col("dst").notEqual("")))
                .distinct();

        System.out.println("Количество ребер в графе: " + edges.count());

        if (edges.count() == 0) {
            System.out.println("Не найдено связей между пользователями");
            return;
        }

        // Создание графа
        System.out.println("Создание графа пользователей...");
        GraphFrame graph = GraphFrame.apply(vertices, edges);

        // Поиск сообществ с помощью Label Propagation Algorithm
        System.out.println("Запуск алгоритма Label Propagation для поиска сообществ...");

        // Расчет статистики по пользователям для каждого сообщества
        Dataset<Row> communityStats = null;
        // Запуск алгоритма распространения меток
        Dataset<Row> communities = graph.labelPropagation().maxIter(3).run();

        try {

            // Создаем временную таблицу для агрегации
            communities.createOrReplaceTempView("communities");
            allTweets.createOrReplaceTempView("all_tweets");

            // SQL запрос для эффективного вычисления статистики
            String sqlQuery = String.format(
                    "SELECT c.label, " +
                            "       COUNT(DISTINCT c.id) as total_users, " +
                            "       SUM(CASE WHEN t.is_political = true THEN 1 ELSE 0 END) as political_tweets, " +
                            "       COUNT(*) as total_tweets, " +
                            "       (SUM(CASE WHEN t.is_political = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as political_ratio " +
                            "FROM communities c " +
                            "JOIN all_tweets t ON c.id = t.userid " +
                            "GROUP BY c.label " +
                            "HAVING political_ratio >= %f " +
                            "ORDER BY political_ratio DESC, total_users DESC " +
                            "LIMIT 20", politicalThreshold * 100);

            communityStats = spark.sql(sqlQuery);
        } catch (Exception e) {
            System.out.println("Ошибка при выполнении Label Propagation: " + e.getMessage());
            System.out.println("Попытка использовать альтернативный подход с ручной агрегацией данных...");

            // Альтернативный подход без Label Propagation
            communityStats = allTweets.groupBy("userid")
                    .agg(
                            count("*").alias("total_tweets"),
                            sum(when(col("is_political").equalTo(true), 1).otherwise(0)).cast(DataTypes.DoubleType).alias("political_tweets")
                    )
                    .withColumn("political_ratio", col("political_tweets").divide(col("total_tweets")))
                    .filter(col("political_ratio").geq(politicalThreshold))
                    .orderBy(col("political_ratio").desc(), col("total_tweets").desc())
                    .limit(20)
                    .withColumnRenamed("userid", "label")
                    .withColumn("total_users", lit(1));
        }

        System.out.printf("Сообщества с долей политического контента >= %.1f%%:\n", politicalThreshold * 100);
        communityStats.show(10);

        if (communityStats.count() == 0) {
            System.out.println("Не найдено сообществ с заданным процентом политических твитов");
            return;
        }

        // Анализ самого крупного политического сообщества
        Row topCommunity = communityStats.first();
        long topCommunityLabel = topCommunity.getAs("label");
        double topCommunityRatio = ((BigDecimal) topCommunity.getAs("political_ratio")).doubleValue();
        long topCommunityUsers = topCommunity.getAs("total_users");
        long totalPoliticalTweets = topCommunity.getAs("political_tweets");
        long totalTweets = topCommunity.getAs("total_tweets");

        System.out.printf("Анализ самого активного политического сообщества (label: %d):\n", topCommunityLabel);
        System.out.printf("Доля политического контента: %.1f%%\n", topCommunityRatio);
        System.out.printf("Количество пользователей: %d\n", topCommunityUsers);
        System.out.printf("Всего твитов в сообществе: %d\n", totalTweets);
        System.out.printf("Политических твитов: %d\n", totalPoliticalTweets);

        // Получение пользователей из этого сообщества
        Dataset<Row> topCommunityUsersDF = null;
        try {
            communities.createOrReplaceTempView("communities");
            String communityUsersQuery = String.format(
                    "SELECT c.id, c.name, c.location, t.total_tweets, t.political_tweets " +
                            "FROM communities c " +
                            "JOIN (SELECT userid, COUNT(*) as total_tweets, " +
                            "      SUM(CASE WHEN is_political THEN 1 ELSE 0 END) as political_tweets " +
                            "      FROM all_tweets GROUP BY userid) t " +
                            "ON c.id = t.userid " +
                            "WHERE c.label = %d " +
                            "ORDER BY t.political_tweets DESC", topCommunityLabel);

            topCommunityUsersDF = spark.sql(communityUsersQuery);
        } catch (Exception e) {
            // Альтернативный подход для получения пользователей
            topCommunityUsersDF = allTweets.filter(col("is_political").equalTo(true))
                    .groupBy("userid", "user_display_name", "user_reported_location")
                    .agg(
                            count("*").alias("total_tweets"),
                            sum(when(col("is_political").equalTo(true), 1).otherwise(0)).alias("political_tweets")
                    )
                    .orderBy(col("political_tweets").desc())
                    .limit(20)
                    .select(
                            col("userid").alias("id"),
                            col("user_display_name").alias("name"),
                            col("user_reported_location").alias("location"),
                            col("total_tweets"),
                            col("political_tweets")
                    );
        }

        System.out.println("Активные пользователи в политическом сообществе:");
        if (topCommunityUsersDF.count() > 0) {
            topCommunityUsersDF.show(Math.min(20, (int) topCommunityUsers));
        }

        // Анализ взаимодействий внутри сообщества
        System.out.println("Анализ взаимодействий внутри сообщества...");
        Dataset<Row> communityEdges = edges
                .filter(col("is_political").equalTo(true))
                .join(topCommunityUsersDF.select(col("id").alias("src_id")), col("src").equalTo(col("src_id")), "left_semi")
                .join(topCommunityUsersDF.select(col("id").alias("dst_id")), col("dst").equalTo(col("dst_id")), "left_semi");

        long communityEdgesCount = communityEdges.count();
        System.out.println("Количество политических взаимодействий внутри сообщества: " + communityEdgesCount);

        if (topCommunityUsers > 0) {
            double avgInteractionsPerUser = communityEdgesCount * 1.0 / topCommunityUsers;
            System.out.printf("Среднее количество политических взаимодействий на пользователя: %.2f\n", avgInteractionsPerUser);
        }
    }
}