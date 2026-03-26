package ru.highload_labs;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import ru.highload_labs.config.AppConfig;
import ru.highload_labs.config.HibernateSessionFactoryProvider;
import ru.highload_labs.domain.DimCategory;
import ru.highload_labs.domain.DimCustomer;
import ru.highload_labs.domain.DimPet;
import ru.highload_labs.domain.DimProduct;
import ru.highload_labs.domain.DimSeller;
import ru.highload_labs.domain.DimStore;
import ru.highload_labs.domain.DimSupplier;
import ru.highload_labs.domain.FactSale;
import ru.highload_labs.domain.RawSaleEvent;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class StarSchemaHibernateSink implements Sink<RawSaleEvent> {
    private final AppConfig appConfig;
    private final int batchSize;

    public StarSchemaHibernateSink(AppConfig appConfig) {
        this.appConfig = appConfig;
        this.batchSize = appConfig.sinkBatchSize();
    }

    @Override
    public SinkWriter<RawSaleEvent> createWriter(WriterInitContext context) {
        return new HibernateStarSinkWriter(appConfig, batchSize);
    }

    private static class HibernateStarSinkWriter implements SinkWriter<RawSaleEvent> {
        private final SessionFactory sessionFactory;
        private final Session session;
        private final int batchSize;

        private Transaction transaction;
        private int currentBatchCount;
        private final Map<String, Long> dimensionKeyCache;

        private HibernateStarSinkWriter(AppConfig appConfig, int batchSize) {
            this.batchSize = batchSize;
            this.sessionFactory = HibernateSessionFactoryProvider.build(appConfig);
            this.session = sessionFactory.openSession();
            this.transaction = session.beginTransaction();
            this.currentBatchCount = 0;
            this.dimensionKeyCache = new HashMap<>();
        }

        @Override
        public void write(RawSaleEvent event, Context context) throws IOException {
            try {
                DimPet pet = resolvePet(event);
                DimCustomer customer = resolveCustomer(event, pet != null ? pet.getId() : null);
                DimSeller seller = resolveSeller(event);
                DimSupplier supplier = resolveSupplier(event);
                DimStore store = resolveStore(event);
                DimCategory category = resolveCategory(event);
                DimProduct product = resolveProduct(event);

                FactSale factSale = FactSale.builder()
                        .saleDate(event.getSaleDate())
                        .saleCustomerId(customer != null ? customer.getId() : null)
                        .saleSellerId(seller != null ? seller.getId() : null)
                        .saleProductId(product != null ? product.getId() : null)
                        .saleQuantity(event.getSaleQuantity())
                        .saleTotalPrice(event.getSaleTotalPrice())
                        .saleSupplierId(supplier != null ? supplier.getId() : null)
                        .saleStoreId(store != null ? store.getId() : null)
                        .saleCategoryId(category != null ? category.getId() : null)
                        .sourceFile(event.getSourceFile())
                        .build();

                session.persist(factSale);
                flushByBatchIfNeeded();
            } catch (Exception ex) {
                log.error("Failed to persist star record", ex);
                rollbackAndRestartTx();
                throw new IOException("Failed to persist star record", ex);
            }
        }

        @Override
        public void flush(boolean endOfInput) throws IOException {
            try {
                if (transaction != null && transaction.isActive()) {
                    transaction.commit();
                }
                session.clear();
                transaction = session.beginTransaction();
                currentBatchCount = 0;
            } catch (Exception ex) {
                rollbackAndRestartTx();
                throw new IOException("Failed to flush sink transaction", ex);
            }
        }

        @Override
        public void close() throws Exception {
            try {
                if (transaction != null && transaction.isActive()) {
                    transaction.commit();
                }
            } catch (Exception ex) {
                log.error("Error while closing sink transaction", ex);
                if (transaction != null && transaction.isActive()) {
                    transaction.rollback();
                }
                throw ex;
            } finally {
                if (session != null) {
                    session.close();
                }
                if (sessionFactory != null) {
                    sessionFactory.close();
                }
            }
        }

        private void flushByBatchIfNeeded() {
            currentBatchCount++;
            if (currentBatchCount >= batchSize) {
                transaction.commit();
                session.clear();
                transaction = session.beginTransaction();
                currentBatchCount = 0;
            }
        }

        private void rollbackAndRestartTx() {
            if (transaction != null && transaction.isActive()) {
                transaction.rollback();
            }
            transaction = session.beginTransaction();
        }

        private DimPet resolvePet(RawSaleEvent event) {
            String key = "pet|" + safe(event.getCustomerPetType()) + "|" + safe(event.getCustomerPetName()) + "|" + safe(event.getCustomerPetBreed()) + "|" + safe(event.getSourceFile());
            if (isBlank(event.getCustomerPetType()) && isBlank(event.getCustomerPetName()) && isBlank(event.getCustomerPetBreed())) {
                return null;
            }

            Long cachedId = dimensionKeyCache.get(key);
            if (cachedId != null) {
                return session.get(DimPet.class, cachedId);
            }

            DimPet found = session.createQuery(
                            "from DimPet p where p.petType = :type and p.petName = :name and p.petBreed = :breed and p.sourceFile = :sourceFile",
                            DimPet.class
                    )
                    .setParameter("type", event.getCustomerPetType())
                    .setParameter("name", event.getCustomerPetName())
                    .setParameter("breed", event.getCustomerPetBreed())
                    .setParameter("sourceFile", event.getSourceFile())
                    .setMaxResults(1)
                    .uniqueResult();

            if (found == null) {
                found = DimPet.builder()
                        .petType(event.getCustomerPetType())
                        .petName(event.getCustomerPetName())
                        .petBreed(event.getCustomerPetBreed())
                        .sourceFile(event.getSourceFile())
                        .build();
                session.persist(found);
                session.flush();
            }

            dimensionKeyCache.put(key, found.getId());
            return found;
        }

        private DimCustomer resolveCustomer(RawSaleEvent event, Long petId) {
            if (event.getSaleCustomerSourceId() == null || isBlank(event.getSourceFile())) {
                return null;
            }

            String key = "customer|" + event.getSaleCustomerSourceId() + "|" + event.getSourceFile();
            Long cachedId = dimensionKeyCache.get(key);
            if (cachedId != null) {
                return session.get(DimCustomer.class, cachedId);
            }

            DimCustomer found = session.createQuery(
                            "from DimCustomer c where c.sourceId = :sourceId and c.sourceFile = :sourceFile",
                            DimCustomer.class
                    )
                    .setParameter("sourceId", event.getSaleCustomerSourceId())
                    .setParameter("sourceFile", event.getSourceFile())
                    .setMaxResults(1)
                    .uniqueResult();

            if (found == null) {
                found = DimCustomer.builder()
                        .firstName(event.getCustomerFirstName())
                        .lastName(event.getCustomerLastName())
                        .age(event.getCustomerAge())
                        .email(event.getCustomerEmail())
                        .country(event.getCustomerCountry())
                        .postalCode(event.getCustomerPostalCode())
                        .petId(petId)
                        .sourceFile(event.getSourceFile())
                        .sourceId(event.getSaleCustomerSourceId())
                        .build();
                session.persist(found);
                session.flush();
            }

            dimensionKeyCache.put(key, found.getId());
            return found;
        }

        private DimSeller resolveSeller(RawSaleEvent event) {
            if (event.getSaleSellerSourceId() == null || isBlank(event.getSourceFile())) {
                return null;
            }

            String key = "seller|" + event.getSaleSellerSourceId() + "|" + event.getSourceFile();
            Long cachedId = dimensionKeyCache.get(key);
            if (cachedId != null) {
                return session.get(DimSeller.class, cachedId);
            }

            DimSeller found = session.createQuery(
                            "from DimSeller s where s.sourceId = :sourceId and s.sourceFile = :sourceFile",
                            DimSeller.class
                    )
                    .setParameter("sourceId", event.getSaleSellerSourceId())
                    .setParameter("sourceFile", event.getSourceFile())
                    .setMaxResults(1)
                    .uniqueResult();

            if (found == null) {
                found = DimSeller.builder()
                        .firstName(event.getSellerFirstName())
                        .lastName(event.getSellerLastName())
                        .email(event.getSellerEmail())
                        .country(event.getSellerCountry())
                        .postalCode(event.getSellerPostalCode())
                        .sourceFile(event.getSourceFile())
                        .sourceId(event.getSaleSellerSourceId())
                        .build();
                session.persist(found);
                session.flush();
            }

            dimensionKeyCache.put(key, found.getId());
            return found;
        }

        private DimSupplier resolveSupplier(RawSaleEvent event) {
            if (isBlank(event.getSupplierEmail()) || isBlank(event.getSourceFile())) {
                return null;
            }

            String key = "supplier|" + safe(event.getSupplierEmail()) + "|" + safe(event.getSourceFile());
            Long cachedId = dimensionKeyCache.get(key);
            if (cachedId != null) {
                return session.get(DimSupplier.class, cachedId);
            }

            DimSupplier found = session.createQuery(
                            "from DimSupplier s where s.supplierEmail = :email and s.sourceFile = :sourceFile",
                            DimSupplier.class
                    )
                    .setParameter("email", event.getSupplierEmail())
                    .setParameter("sourceFile", event.getSourceFile())
                    .setMaxResults(1)
                    .uniqueResult();

            if (found == null) {
                found = DimSupplier.builder()
                        .supplierName(event.getSupplierName())
                        .supplierContact(event.getSupplierContact())
                        .supplierEmail(event.getSupplierEmail())
                        .supplierPhone(event.getSupplierPhone())
                        .supplierAddress(event.getSupplierAddress())
                        .supplierCity(event.getSupplierCity())
                        .supplierCountry(event.getSupplierCountry())
                        .sourceFile(event.getSourceFile())
                        .build();
                session.persist(found);
                session.flush();
            }

            dimensionKeyCache.put(key, found.getId());
            return found;
        }

        private DimStore resolveStore(RawSaleEvent event) {
            if (isBlank(event.getStoreEmail()) || isBlank(event.getSourceFile())) {
                return null;
            }

            String key = "store|" + safe(event.getStoreEmail()) + "|" + safe(event.getSourceFile());
            Long cachedId = dimensionKeyCache.get(key);
            if (cachedId != null) {
                return session.get(DimStore.class, cachedId);
            }

            DimStore found = session.createQuery(
                            "from DimStore s where s.storeEmail = :email and s.sourceFile = :sourceFile",
                            DimStore.class
                    )
                    .setParameter("email", event.getStoreEmail())
                    .setParameter("sourceFile", event.getSourceFile())
                    .setMaxResults(1)
                    .uniqueResult();

            if (found == null) {
                found = DimStore.builder()
                        .storeName(event.getStoreName())
                        .storeLocation(event.getStoreLocation())
                        .storeCity(event.getStoreCity())
                        .storeState(event.getStoreState())
                        .storeCountry(event.getStoreCountry())
                        .storePhone(event.getStorePhone())
                        .storeEmail(event.getStoreEmail())
                        .sourceFile(event.getSourceFile())
                        .build();
                session.persist(found);
                session.flush();
            }

            dimensionKeyCache.put(key, found.getId());
            return found;
        }

        private DimCategory resolveCategory(RawSaleEvent event) {
            String categoryValue = event.getPetCategory();
            if (isBlank(categoryValue)) {
                return null;
            }

            String key = "category|" + categoryValue + "|" + safe(event.getSourceFile());
            Long cachedId = dimensionKeyCache.get(key);
            if (cachedId != null) {
                return session.get(DimCategory.class, cachedId);
            }

            DimCategory found = session.createQuery(
                            "from DimCategory c where c.category = :category and c.sourceFile = :sourceFile",
                            DimCategory.class
                    )
                    .setParameter("category", categoryValue)
                    .setParameter("sourceFile", event.getSourceFile())
                    .setMaxResults(1)
                    .uniqueResult();

            if (found == null) {
                found = DimCategory.builder()
                        .category(categoryValue)
                        .sourceFile(event.getSourceFile())
                        .build();
                session.persist(found);
                session.flush();
            }

            dimensionKeyCache.put(key, found.getId());
            return found;
        }

        private DimProduct resolveProduct(RawSaleEvent event) {
            if (event.getSaleProductSourceId() == null || isBlank(event.getSourceFile())) {
                return null;
            }

            String key = "product|" + event.getSaleProductSourceId() + "|" + event.getSourceFile();
            Long cachedId = dimensionKeyCache.get(key);
            if (cachedId != null) {
                return session.get(DimProduct.class, cachedId);
            }

            DimProduct found = session.createQuery(
                            "from DimProduct p where p.sourceId = :sourceId and p.sourceFile = :sourceFile",
                            DimProduct.class
                    )
                    .setParameter("sourceId", event.getSaleProductSourceId())
                    .setParameter("sourceFile", event.getSourceFile())
                    .setMaxResults(1)
                    .uniqueResult();

            if (found == null) {
                found = DimProduct.builder()
                        .productName(event.getProductName())
                        .productCategory(event.getProductCategory())
                        .productPrice(event.getProductPrice())
                        .productQuantity(event.getProductQuantity())
                        .productWeight(event.getProductWeight())
                        .productColor(event.getProductColor())
                        .productSize(event.getProductSize())
                        .productBrand(event.getProductBrand())
                        .productMaterial(event.getProductMaterial())
                        .productDescription(event.getProductDescription())
                        .productRating(event.getProductRating())
                        .productReviews(event.getProductReviews())
                        .productReleaseDate(event.getProductReleaseDate())
                        .productExpiryDate(event.getProductExpiryDate())
                        .sourceFile(event.getSourceFile())
                        .sourceId(event.getSaleProductSourceId())
                        .build();
                session.persist(found);
                session.flush();
            }

            dimensionKeyCache.put(key, found.getId());
            return found;
        }
    }

    private static String safe(String value) {
        return value == null ? "" : value;
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}



















