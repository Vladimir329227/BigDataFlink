package ru.visoko.flink.star;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.configuration.Configuration;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Types;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.Locale;

/** Upserts dimensions then fact row in one DB transaction per Kafka message. */
public final class PostgresStarSink extends RichSinkFunction<String> {

    private static final long serialVersionUID = 1L;

    private static final DateTimeFormatter SALE_DATE =
            new DateTimeFormatterBuilder()
                    .parseCaseInsensitive()
                    .appendValue(ChronoField.MONTH_OF_YEAR)
                    .appendLiteral('/')
                    .appendValue(ChronoField.DAY_OF_MONTH)
                    .appendLiteral('/')
                    .appendValue(ChronoField.YEAR, 4)
                    .toFormatter(Locale.US);

    private final String jdbcUrl;
    private final String jdbcUser;
    private final String jdbcPassword;

    private transient ObjectMapper mapper;
    private transient Connection connection;

    private transient PreparedStatement psDate;
    private transient PreparedStatement psCustomer;
    private transient PreparedStatement psSeller;
    private transient PreparedStatement psProduct;
    private transient PreparedStatement psStore;
    private transient PreparedStatement psSupplier;
    private transient PreparedStatement psFact;

    public PostgresStarSink(String jdbcUrl, String jdbcUser, String jdbcPassword) {
        this.jdbcUrl = jdbcUrl;
        this.jdbcUser = jdbcUser;
        this.jdbcPassword = jdbcPassword;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        mapper = new ObjectMapper();
        connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
        connection.setAutoCommit(false);

        psDate =
                connection.prepareStatement(
                        "INSERT INTO dim_date (date_key, full_date, year_num, month_num, day_num) "
                                + "VALUES (?,?,?,?,?) ON CONFLICT (date_key) DO NOTHING");

        psCustomer =
                connection.prepareStatement(
                        "INSERT INTO dim_customer (customer_id, first_name, last_name, age, email, country, "
                                + "postal_code, pet_type, pet_name, pet_breed) VALUES (?,?,?,?,?,?,?,?,?,?) "
                                + "ON CONFLICT (customer_id) DO UPDATE SET first_name = EXCLUDED.first_name, "
                                + "last_name = EXCLUDED.last_name, age = EXCLUDED.age, email = EXCLUDED.email, "
                                + "country = EXCLUDED.country, postal_code = EXCLUDED.postal_code, "
                                + "pet_type = EXCLUDED.pet_type, pet_name = EXCLUDED.pet_name, "
                                + "pet_breed = EXCLUDED.pet_breed");

        psSeller =
                connection.prepareStatement(
                        "INSERT INTO dim_seller (seller_id, first_name, last_name, email, country, postal_code) "
                                + "VALUES (?,?,?,?,?,?) ON CONFLICT (seller_id) DO UPDATE SET "
                                + "first_name = EXCLUDED.first_name, last_name = EXCLUDED.last_name, "
                                + "email = EXCLUDED.email, country = EXCLUDED.country, "
                                + "postal_code = EXCLUDED.postal_code");

        psProduct =
                connection.prepareStatement(
                        "INSERT INTO dim_product (product_id, product_name, product_category, product_price, "
                                + "product_quantity, pet_category, product_weight, product_color, product_size, "
                                + "product_brand, product_material, product_description, product_rating, "
                                + "product_reviews, product_release_date, product_expiry_date) "
                                + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT (product_id) DO UPDATE SET "
                                + "product_name = EXCLUDED.product_name, product_category = EXCLUDED.product_category, "
                                + "product_price = EXCLUDED.product_price, product_quantity = EXCLUDED.product_quantity, "
                                + "pet_category = EXCLUDED.pet_category, product_weight = EXCLUDED.product_weight, "
                                + "product_color = EXCLUDED.product_color, product_size = EXCLUDED.product_size, "
                                + "product_brand = EXCLUDED.product_brand, product_material = EXCLUDED.product_material, "
                                + "product_description = EXCLUDED.product_description, product_rating = EXCLUDED.product_rating, "
                                + "product_reviews = EXCLUDED.product_reviews, product_release_date = EXCLUDED.product_release_date, "
                                + "product_expiry_date = EXCLUDED.product_expiry_date");

        psStore =
                connection.prepareStatement(
                        "INSERT INTO dim_store (store_key, store_name, store_location, store_city, store_state, "
                                + "store_country, store_phone, store_email) VALUES (?,?,?,?,?,?,?,?) "
                                + "ON CONFLICT (store_key) DO UPDATE SET store_name = EXCLUDED.store_name, "
                                + "store_location = EXCLUDED.store_location, store_city = EXCLUDED.store_city, "
                                + "store_state = EXCLUDED.store_state, store_country = EXCLUDED.store_country, "
                                + "store_phone = EXCLUDED.store_phone, store_email = EXCLUDED.store_email");

        psSupplier =
                connection.prepareStatement(
                        "INSERT INTO dim_supplier (supplier_email, supplier_name, supplier_contact, supplier_phone, "
                                + "supplier_address, supplier_city, supplier_country) VALUES (?,?,?,?,?,?,?) "
                                + "ON CONFLICT (supplier_email) DO UPDATE SET supplier_name = EXCLUDED.supplier_name, "
                                + "supplier_contact = EXCLUDED.supplier_contact, supplier_phone = EXCLUDED.supplier_phone, "
                                + "supplier_address = EXCLUDED.supplier_address, supplier_city = EXCLUDED.supplier_city, "
                                + "supplier_country = EXCLUDED.supplier_country");

        psFact =
                connection.prepareStatement(
                        "INSERT INTO fact_sales (sale_id, date_key, customer_id, seller_id, product_id, store_key, "
                                + "supplier_email, sale_quantity, sale_total_price) VALUES (?,?,?,?,?,?,?,?,?) "
                                + "ON CONFLICT (sale_id) DO UPDATE SET date_key = EXCLUDED.date_key, "
                                + "customer_id = EXCLUDED.customer_id, seller_id = EXCLUDED.seller_id, "
                                + "product_id = EXCLUDED.product_id, store_key = EXCLUDED.store_key, "
                                + "supplier_email = EXCLUDED.supplier_email, sale_quantity = EXCLUDED.sale_quantity, "
                                + "sale_total_price = EXCLUDED.sale_total_price");
    }

    @Override
    public void invoke(String json, Context context) throws Exception {
        JsonNode n = mapper.readTree(json);
        try {
            writeRow(n);
            connection.commit();
        } catch (Exception e) {
            connection.rollback();
            throw e;
        }
    }

    private void writeRow(JsonNode n) throws Exception {
        long saleId =
                n.hasNonNull("stream_sale_id")
                        ? n.get("stream_sale_id").asLong()
                        : n.path("id").asLong();
        LocalDate saleLocalDate = parseSaleDate(text(n, "sale_date"));
        if (saleLocalDate == null) {
            saleLocalDate = LocalDate.of(1970, 1, 1);
        }
        int dateKey = saleLocalDate.getYear() * 10000 + saleLocalDate.getMonthValue() * 100 + saleLocalDate.getDayOfMonth();

        bindDate(dateKey, saleLocalDate);
        psDate.executeUpdate();

        int customerId = n.path("sale_customer_id").asInt();
        bindCustomer(
                customerId,
                text(n, "customer_first_name"),
                text(n, "customer_last_name"),
                intOrNull(n, "customer_age"),
                text(n, "customer_email"),
                text(n, "customer_country"),
                text(n, "customer_postal_code"),
                text(n, "customer_pet_type"),
                text(n, "customer_pet_name"),
                text(n, "customer_pet_breed"));
        psCustomer.executeUpdate();

        int sellerId = n.path("sale_seller_id").asInt();
        bindSeller(
                sellerId,
                text(n, "seller_first_name"),
                text(n, "seller_last_name"),
                text(n, "seller_email"),
                text(n, "seller_country"),
                text(n, "seller_postal_code"));
        psSeller.executeUpdate();

        int productId = n.path("sale_product_id").asInt();
        bindProduct(
                productId,
                text(n, "product_name"),
                text(n, "product_category"),
                doubleOrNull(n, "product_price"),
                intOrNull(n, "product_quantity"),
                text(n, "pet_category"),
                doubleOrNull(n, "product_weight"),
                text(n, "product_color"),
                text(n, "product_size"),
                text(n, "product_brand"),
                text(n, "product_material"),
                text(n, "product_description"),
                doubleOrNull(n, "product_rating"),
                longOrNull(n, "product_reviews"),
                text(n, "product_release_date"),
                text(n, "product_expiry_date"));
        psProduct.executeUpdate();

        String storeKey = storeKey(text(n, "store_name"), text(n, "store_phone"), text(n, "store_email"));
        bindStore(
                storeKey,
                text(n, "store_name"),
                text(n, "store_location"),
                text(n, "store_city"),
                text(n, "store_state"),
                text(n, "store_country"),
                text(n, "store_phone"),
                text(n, "store_email"));
        psStore.executeUpdate();

        String supplierEmail = supplierEmailKey(text(n, "supplier_email"), text(n, "supplier_name"));
        bindSupplier(
                supplierEmail,
                text(n, "supplier_name"),
                text(n, "supplier_contact"),
                text(n, "supplier_phone"),
                text(n, "supplier_address"),
                text(n, "supplier_city"),
                text(n, "supplier_country"));
        psSupplier.executeUpdate();

        bindFact(
                saleId,
                dateKey,
                customerId,
                sellerId,
                productId,
                storeKey,
                supplierEmail,
                intOrNull(n, "sale_quantity"),
                doubleOrNull(n, "sale_total_price"));
        psFact.executeUpdate();
    }

    private void bindDate(int dateKey, LocalDate d) throws Exception {
        psDate.setInt(1, dateKey);
        psDate.setDate(2, Date.valueOf(d));
        psDate.setInt(3, d.getYear());
        psDate.setInt(4, d.getMonthValue());
        psDate.setInt(5, d.getDayOfMonth());
    }

    private void bindCustomer(
            int id,
            String fn,
            String ln,
            Integer age,
            String email,
            String country,
            String postal,
            String petType,
            String petName,
            String petBreed)
            throws Exception {
        psCustomer.setInt(1, id);
        psCustomer.setString(2, emptyToNull(fn));
        psCustomer.setString(3, emptyToNull(ln));
        setIntOrNull(psCustomer, 4, age);
        psCustomer.setString(5, emptyToNull(email));
        psCustomer.setString(6, emptyToNull(country));
        psCustomer.setString(7, emptyToNull(postal));
        psCustomer.setString(8, emptyToNull(petType));
        psCustomer.setString(9, emptyToNull(petName));
        psCustomer.setString(10, emptyToNull(petBreed));
    }

    private void bindSeller(int id, String fn, String ln, String email, String country, String postal)
            throws Exception {
        psSeller.setInt(1, id);
        psSeller.setString(2, emptyToNull(fn));
        psSeller.setString(3, emptyToNull(ln));
        psSeller.setString(4, emptyToNull(email));
        psSeller.setString(5, emptyToNull(country));
        psSeller.setString(6, emptyToNull(postal));
    }

    private void bindProduct(
            int id,
            String name,
            String category,
            Double price,
            Integer qty,
            String petCat,
            Double weight,
            String color,
            String size,
            String brand,
            String material,
            String description,
            Double rating,
            Long reviews,
            String release,
            String expiry)
            throws Exception {
        psProduct.setInt(1, id);
        psProduct.setString(2, emptyToNull(name));
        psProduct.setString(3, emptyToNull(category));
        setDoubleOrNull(psProduct, 4, price);
        setIntOrNull(psProduct, 5, qty);
        psProduct.setString(6, emptyToNull(petCat));
        setDoubleOrNull(psProduct, 7, weight);
        psProduct.setString(8, emptyToNull(color));
        psProduct.setString(9, emptyToNull(size));
        psProduct.setString(10, emptyToNull(brand));
        psProduct.setString(11, emptyToNull(material));
        psProduct.setString(12, emptyToNull(description));
        setDoubleOrNull(psProduct, 13, rating);
        setLongOrNull(psProduct, 14, reviews);
        psProduct.setString(15, emptyToNull(release));
        psProduct.setString(16, emptyToNull(expiry));
    }

    private void bindStore(
            String key,
            String name,
            String loc,
            String city,
            String state,
            String country,
            String phone,
            String email)
            throws Exception {
        psStore.setString(1, key);
        psStore.setString(2, emptyToNull(name));
        psStore.setString(3, emptyToNull(loc));
        psStore.setString(4, emptyToNull(city));
        psStore.setString(5, emptyToNull(state));
        psStore.setString(6, emptyToNull(country));
        psStore.setString(7, emptyToNull(phone));
        psStore.setString(8, emptyToNull(email));
    }

    private void bindSupplier(
            String emailKey,
            String name,
            String contact,
            String phone,
            String address,
            String city,
            String country)
            throws Exception {
        psSupplier.setString(1, emailKey);
        psSupplier.setString(2, emptyToNull(name));
        psSupplier.setString(3, emptyToNull(contact));
        psSupplier.setString(4, emptyToNull(phone));
        psSupplier.setString(5, emptyToNull(address));
        psSupplier.setString(6, emptyToNull(city));
        psSupplier.setString(7, emptyToNull(country));
    }

    private void bindFact(
            long saleId,
            int dateKey,
            int customerId,
            int sellerId,
            int productId,
            String storeKey,
            String supplierEmail,
            Integer qty,
            Double total)
            throws Exception {
        psFact.setLong(1, saleId);
        psFact.setInt(2, dateKey);
        psFact.setInt(3, customerId);
        psFact.setInt(4, sellerId);
        psFact.setInt(5, productId);
        psFact.setString(6, storeKey);
        psFact.setString(7, supplierEmail);
        setIntOrNull(psFact, 8, qty);
        if (total != null) {
            psFact.setBigDecimal(9, java.math.BigDecimal.valueOf(total));
        } else {
            psFact.setNull(9, Types.NUMERIC);
        }
    }

    private static void setIntOrNull(PreparedStatement ps, int i, Integer v) throws Exception {
        if (v == null) {
            ps.setNull(i, Types.INTEGER);
        } else {
            ps.setInt(i, v);
        }
    }

    private static void setLongOrNull(PreparedStatement ps, int i, Long v) throws Exception {
        if (v == null) {
            ps.setNull(i, Types.BIGINT);
        } else {
            ps.setLong(i, v);
        }
    }

    private static void setDoubleOrNull(PreparedStatement ps, int i, Double v) throws Exception {
        if (v == null) {
            ps.setNull(i, Types.DOUBLE);
        } else {
            ps.setDouble(i, v);
        }
    }

    private static String text(JsonNode n, String field) {
        JsonNode v = n.get(field);
        if (v == null || v.isNull()) {
            return "";
        }
        return v.asText("");
    }

    private static Integer intOrNull(JsonNode n, String field) {
        JsonNode v = n.get(field);
        if (v == null || v.isNull() || v.asText("").isEmpty()) {
            return null;
        }
        return v.asInt();
    }

    private static Long longOrNull(JsonNode n, String field) {
        JsonNode v = n.get(field);
        if (v == null || v.isNull() || v.asText("").isEmpty()) {
            return null;
        }
        return v.asLong();
    }

    private static Double doubleOrNull(JsonNode n, String field) {
        JsonNode v = n.get(field);
        if (v == null || v.isNull() || v.asText("").isEmpty()) {
            return null;
        }
        return v.asDouble();
    }

    private static String emptyToNull(String s) {
        return s == null || s.isEmpty() ? null : s;
    }

    private static LocalDate parseSaleDate(String raw) {
        if (raw == null || raw.isEmpty()) {
            return null;
        }
        try {
            return LocalDate.parse(raw.trim(), SALE_DATE);
        } catch (DateTimeParseException e) {
            return null;
        }
    }

    private static String storeKey(String name, String phone, String email) {
        String n = name == null ? "" : name;
        String p = phone == null ? "" : phone;
        String e = email == null ? "" : email;
        String key = n + "|" + p + "|" + e;
        if (key.length() > 500) {
            return key.substring(0, 500);
        }
        return key.isEmpty() ? "UNKNOWN_STORE" : key;
    }

    private static String supplierEmailKey(String email, String supplierName) {
        if (email != null && !email.isEmpty()) {
            return email.length() > 512 ? email.substring(0, 512) : email;
        }
        String fallback = "unknown+" + (supplierName == null ? "supplier" : supplierName.replaceAll("\\s+", "_"));
        return fallback.length() > 512 ? fallback.substring(0, 512) : fallback;
    }

    @Override
    public void close() throws Exception {
        if (psDate != null) {
            psDate.close();
        }
        if (psCustomer != null) {
            psCustomer.close();
        }
        if (psSeller != null) {
            psSeller.close();
        }
        if (psProduct != null) {
            psProduct.close();
        }
        if (psStore != null) {
            psStore.close();
        }
        if (psSupplier != null) {
            psSupplier.close();
        }
        if (psFact != null) {
            psFact.close();
        }
        if (connection != null) {
            connection.close();
        }
        super.close();
    }
}
