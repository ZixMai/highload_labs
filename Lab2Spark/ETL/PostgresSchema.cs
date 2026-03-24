using Npgsql;

namespace Lab2Spark.ETL;

public static class PostgresSchema
{
    private static readonly string[] DefaultTables =
    [
        "dim_category", "dim_pet", "dim_customer", "dim_seller",
        "dim_product", "dim_store", "dim_supplier", "fact_sale"
    ];

    private const string DefaultConnectionString =
        "Host=postgres;Port=5432;Database=postgres;Username=postgres;Password=postgres";

    private static readonly (string ConstraintName, string SourceColumn, string TargetTable)[] FactSaleForeignKeys =
    [
        ("fact_sale_customer_fk", "customer_id", "dim_customer"),
        ("fact_sale_seller_fk", "seller_id", "dim_seller"),
        ("fact_sale_product_fk", "product_id", "dim_product"),
        ("fact_sale_store_fk", "store_id", "dim_store"),
        ("fact_sale_supplier_fk", "supplier_id", "dim_supplier"),
        ("fact_sale_category_fk", "category_id", "dim_category")
    ];

    private static readonly (string ConstraintName, string SourceColumn, string TargetTable)[] DimCustomerForeignKeys =
    [
        ("dim_customer_pet_fk", "pet_id", "dim_pet")
    ];

    public static async Task CreatePrimaryKeys(string? connectionString = null, params string[] tables)
    {
        var cs = connectionString ?? DefaultConnectionString;
        var targetTables = tables.Length == 0 ? DefaultTables : tables;

        await using var conn = new NpgsqlConnection(cs);
        await conn.OpenAsync();

        foreach (var table in targetTables)
        {
            try
            {
                await EnsurePrimaryKeyAsync(conn, table);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[PK] Failed for table '{table}': {ex.Message}");
            }
        }
    }

    public static async Task CreateForeignKeys(string? connectionString = null, string factTable = "fact_sale")
    {
        var cs = connectionString ?? DefaultConnectionString;

        await using var conn = new NpgsqlConnection(cs);
        await conn.OpenAsync();

        foreach (var fk in FactSaleForeignKeys)
        {
            try
            {
                await EnsureForeignKeyAsync(conn, factTable, fk.ConstraintName, fk.SourceColumn, fk.TargetTable);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[FK] Failed for '{fk.ConstraintName}': {ex.Message}");
            }
        }

        foreach (var fk in DimCustomerForeignKeys)
        {
            try
            {
                await EnsureForeignKeyAsync(conn, "dim_customer", fk.ConstraintName, fk.SourceColumn, fk.TargetTable);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[FK] Failed for '{fk.ConstraintName}': {ex.Message}");
            }
        }
    }

    public static async Task DropForeignKeys(string? connectionString = null, string factTable = "fact_sale")
    {
        var cs = connectionString ?? DefaultConnectionString;

        await using var conn = new NpgsqlConnection(cs);
        await conn.OpenAsync();

        try
        {
            await DropAllForeignKeysForTableAsync(conn, factTable);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[FK] Drop failed for table '{factTable}': {ex.Message}");
        }

        try
        {
            await DropAllForeignKeysForTableAsync(conn, "dim_customer");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[FK] Drop failed for table 'dim_customer': {ex.Message}");
        }
    }

    private static async Task EnsurePrimaryKeyAsync(NpgsqlConnection conn, string table)
    {
        var qTable = QuoteIdentifier(table);
        var seqName = $"{table}_id_seq";
        var qSeq = QuoteIdentifier(seqName);
        var pkName = $"{table}_pkey";
        var qPk = QuoteIdentifier(pkName);

        await ExecuteNonQueryAsync(conn, $"ALTER TABLE {qTable} ADD COLUMN IF NOT EXISTS id BIGINT;");

        await ExecuteNonQueryAsync(conn, $"CREATE SEQUENCE IF NOT EXISTS {qSeq};");
        await ExecuteNonQueryAsync(conn, $"ALTER TABLE {qTable} ALTER COLUMN id SET DEFAULT nextval('{seqName}');");

        await ExecuteNonQueryAsync(conn, $"UPDATE {qTable} SET id = nextval('{seqName}') WHERE id IS NULL;");

        await ExecuteNonQueryAsync(conn, $@"
WITH d AS (
    SELECT ctid
    FROM (
        SELECT ctid, ROW_NUMBER() OVER (PARTITION BY id ORDER BY ctid) AS rn
        FROM {qTable}
    ) x
    WHERE x.rn > 1
)
UPDATE {qTable} t
SET id = nextval('{seqName}')
FROM d
WHERE t.ctid = d.ctid;");

        await ExecuteNonQueryAsync(conn, $"SELECT setval('{seqName}', COALESCE((SELECT MAX(id) FROM {qTable}), 0), true);");

        await ExecuteNonQueryAsync(conn, $"ALTER TABLE {qTable} ALTER COLUMN id SET NOT NULL;");
        await ExecuteNonQueryAsync(conn, $@"
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = '{pkName}'
    ) THEN
        ALTER TABLE {qTable} ADD CONSTRAINT {qPk} PRIMARY KEY (id);
    END IF;
END $$;");
    }

    private static async Task DropAllForeignKeysForTableAsync(NpgsqlConnection conn, string sourceTable)
    {
        await ExecuteNonQueryAsync(conn, $@"
DO $$
DECLARE
    r record;
BEGIN
    IF to_regclass('public.{sourceTable}') IS NOT NULL THEN
        FOR r IN
            SELECT c.conname
            FROM pg_constraint c
            JOIN pg_class t ON t.oid = c.conrelid
            WHERE c.contype = 'f' AND t.relname = '{sourceTable}'
        LOOP
            EXECUTE format('ALTER TABLE %I DROP CONSTRAINT IF EXISTS %I', '{sourceTable}', r.conname);
        END LOOP;
    END IF;
END $$;");
    }

    private static async Task EnsureForeignKeyAsync(
        NpgsqlConnection conn,
        string sourceTable,
        string constraintName,
        string sourceColumn,
        string targetTable)
    {
        var qSource = QuoteIdentifier(sourceTable);
        var qConstraint = QuoteIdentifier(constraintName);
        var qSourceColumn = QuoteIdentifier(sourceColumn);
        var qTarget = QuoteIdentifier(targetTable);

        await ExecuteNonQueryAsync(conn, $@"
DO $$
BEGIN
    IF
        to_regclass('public.{sourceTable}') IS NOT NULL
        AND to_regclass('public.{targetTable}') IS NOT NULL
        AND EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = '{sourceTable}' AND column_name = '{sourceColumn}'
        )
        AND EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = '{targetTable}' AND column_name = 'id'
        )
        AND NOT EXISTS (
            SELECT 1
            FROM pg_constraint c
            JOIN pg_class t ON t.oid = c.conrelid
            WHERE c.conname = '{constraintName}' AND t.relname = '{sourceTable}'
        )
    THEN
        ALTER TABLE {qSource}
        ADD CONSTRAINT {qConstraint}
        FOREIGN KEY ({qSourceColumn})
        REFERENCES {qTarget}(id);
    END IF;
END $$;");
    }

    private static async Task ExecuteNonQueryAsync(NpgsqlConnection conn, string sql)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandTimeout = 120;
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static string QuoteIdentifier(string identifier) =>
        $"\"{identifier.Replace("\"", "\"\"")}\"";
}
