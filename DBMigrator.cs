using System.Data;
using System.Data.Common;
using System.Reflection;
using System.Text.RegularExpressions;
using Bvb.Framework.CodeDb;
using Bvb.Framework.Database;
using Bvb.Framework.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Bvb.Framework.Server;

public interface IDbMigrator
{
    Task MigrateDbs(CancellationToken cancellationToken);
    Task EnsureMigrated(CancellationToken cancellationToken);
}

internal class DbMigrator<TDb> : IDbMigrator
    where TDb : DomainDb
{
    private readonly TDb _db;
    private readonly CodeDb.CodeDb _codeDb;
    private readonly DomainLogDb? _logDb;
    private readonly DomainArchiveDb? _archiveDb;
    private readonly ICodeService _codeService;
    private readonly IConfiguration _configuration;
    private readonly ILogger<DbMigrator<TDb>> _logger;

    public DbMigrator(
        TDb db, CodeDb.CodeDb codeDb, ICodeService codeService,
        IConfiguration configuration, ILogger<DbMigrator<TDb>> logger,
        DomainLogDb? logDb = null, DomainArchiveDb? archiveDb = null)
    {
        _db = db;
        _codeDb = codeDb;
        _codeService = codeService;
        _configuration = configuration;
        _logger = logger; 
        _logDb = logDb;
        _archiveDb = archiveDb;
    }

    public async Task EnsureMigrated(CancellationToken cancellationToken)
    {
        if (IsRemote() && !IsTest())
        {
            await RequireNoPendingMigrations(cancellationToken);
        }
        else
        {
            await MigrateDbs(cancellationToken);
        }
    }

    private bool IsTest()
    {
        string connectionString = _db.Database.GetConnectionString() ??
                                  throw new InvalidOperationException("Database has no connection string");
        return ConnectionHelper.IsTest(connectionString);
    }

    private bool IsRemote()
    {
        string connectionString = _db.Database.GetConnectionString() ??
                                  throw new InvalidOperationException("Database has no connection string");
        return ConnectionHelper.IsRemote(connectionString);
    }

    private async Task RequireNoPendingMigrations(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Retrieving pending Db and CodeDb migrations...");

        IEnumerable<string> logMigrations = _logDb != null
            ? await _logDb.Database.GetPendingMigrationsAsync(cancellationToken)
            : Enumerable.Empty<string>();

        IEnumerable<string> archiveMigrations = _archiveDb != null
            ? await _archiveDb.Database.GetPendingMigrationsAsync(cancellationToken)
            : Enumerable.Empty<string>();

        IEnumerable<string> pending = (await _db.Database
                .GetPendingMigrationsAsync(cancellationToken))
            .Concat(logMigrations)
            .Concat(archiveMigrations)
            .Concat(await _codeDb.Database
                .GetPendingMigrationsAsync(cancellationToken))
            .ToList();

        if (pending.Any())
        {
            throw new InvalidOperationException(
                $"There are {pending.Count()} pending migrations: {string.Join(", ", pending)}");
        }

        _logger.LogInformation("No migrations are pending");
    }

    public async Task MigrateDbs(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Database migration started.");

        await MigrateCodeDb(cancellationToken);
        await MigrateDb(_logDb, "LogDb", cancellationToken);
        await MigrateDb(_archiveDb, "ArchiveDb", cancellationToken);

        _db.Database.SetCommandTimeout(300);
        if (IsRemote())
        {
            await ClearDb(cancellationToken);
            await RefreshCrossDomain(cancellationToken);
        }
        await MigrateDb(cancellationToken);
        if (IsRemote())
        {
            await RefreshViews(cancellationToken);
            await RefreshFunctions(cancellationToken);
            await RefreshStoredProcedures(cancellationToken);
            await RefreshCrossDomain(cancellationToken);
            await ExecuteScripts(cancellationToken);
            await SetCdcEnabled(cancellationToken);
            await SetObfuscateEnabled(cancellationToken);
            await SetObfuscationExceptions(cancellationToken);
        }
        _logger.LogInformation("Database migration finished.");
    }

    private async Task MigrateCodeDb(CancellationToken cancellationToken)
    {
        if (_codeDb.Database.CurrentTransaction != null)
        {
            _logger.LogWarning("Cannot migrate CodeDb if a transaction has already been started.");
            return;
        }

        _logger.LogInformation("Retrieving pending Code Db migrations...");

        IEnumerable<string> pending = (await _codeDb.Database
            .GetPendingMigrationsAsync(cancellationToken)).ToList();

        if (pending.Any())
        {
            _logger.LogInformation($"Executing {pending.Count()} pending Code Db migrations: {string.Join(", ", pending)}");
            _codeDb.Database.SetCommandTimeout(300);
            await _codeDb.Database.MigrateAsync(cancellationToken);
            _logger.LogInformation($"Successfully executed {pending.Count()} pending migrations.");
        }
        else
        {
            _logger.LogInformation("No Code Db migrations are pending.");
        }

        _logger.LogInformation("Saving Codes to the Code Db...");
        int saved = await _codeService.SaveCodesToDb(cancellationToken);
        _logger.LogInformation($"{saved} Codes affected in the Code Db...");
    }
    
    private async Task MigrateDb(DbContext? db, string type, CancellationToken cancellationToken)
    {
        if (db == null)
        {
            _logger.LogInformation($"No {type} configured.");
            return;
        }

        if (db.Database.CurrentTransaction != null)
        {
            _logger.LogWarning($"Cannot migrate {type} if a transaction has already been started.");
            return;
        }

        _logger.LogInformation($"Retrieving pending {type} migrations...");

        IEnumerable<string> pending = (await db.Database
            .GetPendingMigrationsAsync(cancellationToken)).ToList();

        if (pending.Any())
        {
            _logger.LogInformation($"Executing {pending.Count()} pending {type} migrations: {string.Join(", ", pending)}");
            db.Database.SetCommandTimeout(300);
            await db.Database.MigrateAsync(cancellationToken);
            _logger.LogInformation($"Successfully executed {pending.Count()} pending migrations.");
        }
        else
        {
            _logger.LogInformation($"No {type} migrations are pending.");
        }
    }

    internal static async Task ExecuteSquashedMigrations(DbContext db, ILogger logger, CancellationToken cancellationToken)
    {
        string squashedPath = Path.Combine(AppContext.BaseDirectory, "Migrations", "Squashed");

        if (!Directory.Exists(squashedPath))
        {
            logger.LogInformation($"Squashed Migrations path not found: {squashedPath}");
            return;
        }

        string[] files = Directory.GetFiles(squashedPath, "*.sql", SearchOption.AllDirectories);

        foreach (string file in files.OrderBy(x => x))
        {
            string script = await File.ReadAllTextAsync(file, cancellationToken);
            var commands = Regex
                .Split(script, @"^\s*GO\s*$", RegexOptions.Multiline | RegexOptions.IgnoreCase)
                .Select(s => s.Trim())
                .Where(sql => sql is not ("" or "BEGIN TRANSACTION;" or "COMMIT;"))
                .ToList();

            string lastMigration = commands
                .Last(sql => sql.Contains("INSERT INTO [__EFMigrationsHistory]"));

            lastMigration = lastMigration[(lastMigration.IndexOf("N'") + 2)..];
            lastMigration = lastMigration[..lastMigration.IndexOf("'")];

            DbConnection connection = db.Database.GetDbConnection();
            DbCommand query = connection.CreateCommand();
            query.CommandText = $"SELECT COUNT(*) FROM [__EFMigrationsHistory] WHERE [MigrationId] = N'{lastMigration}'";

            if (connection.State != ConnectionState.Open)
            {
                await connection.OpenAsync(cancellationToken);
            }

            int? applied = (int?)await query.ExecuteScalarAsync(cancellationToken);

            if (applied > 0)
            {
                logger.LogInformation($"Squashed migrations {Path.GetFileName(file)} already applied.");
                continue;
            }

            logger.LogInformation($"Executing squashed migrations {Path.GetFileName(file)}");

            foreach (string sql in commands)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    break;
                }

                try
                {
                    DbCommand command = connection.CreateCommand();
                    command.CommandText = sql;
                    await command.ExecuteNonQueryAsync(cancellationToken);
                }
                catch (Exception e)
                {
                    logger.LogError(e, "Failed to execute SQL: {0}", sql);
                    throw;
                }
            }
        }
    }

    private async Task ClearDb(CancellationToken cancellationToken)
    {
        if (_configuration.GetValue("ClearStoredProcedures", false))
        {
            _logger.LogInformation("Executing ClearStoredProcedures");

            await _db.Database.ExecuteSqlRawAsync(@"DECLARE @itemSchema varchar(50), @itemName varchar(500)
DECLARE someCursor CURSOR FOR
    SELECT SCHEMA_NAME(schema_id),name FROM sys.procedures AS P WHERE OBJECTPROPERTY(object_id, 'IsMSShipped') = 0 AND P.name <> 'PumpMessagesIntraday'
    
	OPEN someCursor
		FETCH NEXT FROM someCursor INTO @itemSchema, @itemName
		WHILE @@FETCH_STATUS = 0
			BEGIN
				EXEC('drop proc [' +@itemSchema+ '].[' + @itemName + ']') 
				FETCH NEXT FROM someCursor INTO @itemSchema, @itemName
			END
	CLOSE someCursor
DEALLOCATE someCursor", cancellationToken);
        }
        else
        {
            _logger.LogInformation("Skipping ClearStoredProcedures");
        }

        if (_configuration.GetValue("ClearFunctions", false))
        {
            _logger.LogInformation("Executing ClearFunctions");

            await _db.Database.ExecuteSqlRawAsync(@"DECLARE @itemSchema varchar(50), @itemName varchar(500)
DECLARE someCursor CURSOR FOR
    SELECT SPECIFIC_SCHEMA, SPECIFIC_NAME FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE = 'FUNCTION' 
    AND SPECIFIC_NAME not in ('fn_diagramobjects', 'sp_alterdiagram', 'sp_creatediagram', 'sp_dropdiagram', 
    'sp_helpdiagramdefinition', 'sp_helpdiagrams', 'sp_renamediagram', 'sp_upgraddiagrams', 'sysdiagrams')   
	OPEN someCursor
		FETCH NEXT FROM someCursor INTO @itemSchema, @itemName
		WHILE @@FETCH_STATUS = 0
			BEGIN
				EXEC('drop function [' +@itemSchema+ '].[' + @itemName + ']') 
				FETCH NEXT FROM someCursor INTO @itemSchema, @itemName
			END
	CLOSE someCursor
DEALLOCATE someCursor", cancellationToken);
        }
        else
        {
            _logger.LogInformation("Skipping ClearFunctions");
        }

        if (_configuration.GetValue("ClearViews", false))
        {
            _logger.LogInformation("Executing ClearViews");

            await _db.Database.ExecuteSqlRawAsync(@"DECLARE @itemSchema varchar(50), @itemName varchar(500)
DECLARE someCursor CURSOR FOR
    SELECT SCHEMA_NAME(schema_id),name FROM sys.views WHERE OBJECTPROPERTY(object_id, 'IsMSShipped') = 0
	OPEN someCursor
		FETCH NEXT FROM someCursor INTO @itemSchema, @itemName
		WHILE @@FETCH_STATUS = 0
			BEGIN
				EXEC('drop view [' +@itemSchema+ '].[' + @itemName + ']') 
				FETCH NEXT FROM someCursor INTO @itemSchema, @itemName
			END
	CLOSE someCursor
DEALLOCATE someCursor", cancellationToken);
        }
        else
        {
            _logger.LogInformation("Skipping ClearViews");
        }
    }

    private async Task MigrateDb(CancellationToken cancellationToken)
    {
        if (_db.Database.CurrentTransaction != null)
        {
            _logger.LogWarning("Cannot migrate Db if a transaction has already been started.");
            return;
        }

        if (!IsRemote())
        {
            await ExecuteSquashedMigrations(_db, _logger, cancellationToken);
        }

        _logger.LogInformation("Retrieving pending Db migrations...");

        IEnumerable<string> pending = (await _db.Database
                .GetPendingMigrationsAsync(cancellationToken))
            .ToList();

        if (pending.Any())
        {
            _logger.LogInformation($"Executing {pending.Count()} pending migrations: {string.Join(", ", pending)}");
            _db.Database.SetCommandTimeout(7200);
            await _db.Database.MigrateAsync(cancellationToken);
            _logger.LogInformation($"Successfully executed {pending.Count()} pending migrations.");
        }
        else
        {
            _logger.LogInformation("No Db migrations are pending.");
        }
    }

    private async Task RefreshCrossDomain(CancellationToken cancellationToken)
    {
        await ExecuteSlqFilesForDirectory(Path.Combine(AppContext.BaseDirectory, "Migrations", "CrossDomain"), cancellationToken);
    }

    private async Task RefreshViews(CancellationToken cancellationToken)
    {
        await ExecuteSlqFilesForDirectory(Path.Combine(AppContext.BaseDirectory, "Migrations", "Views"), cancellationToken);
    }

    private async Task RefreshFunctions(CancellationToken cancellationToken)
    {
        await ExecuteSlqFilesForDirectory(Path.Combine(AppContext.BaseDirectory, "Migrations", "Functions"), cancellationToken);
    }

    private async Task RefreshStoredProcedures(CancellationToken cancellationToken)
    {
        await ExecuteSlqFilesForDirectory(Path.Combine(AppContext.BaseDirectory, "Migrations", "StoredProcedures"), cancellationToken);
    }

    private async Task ExecuteScripts(CancellationToken cancellationToken)
    {
        await ExecuteSlqFilesForDirectory(Path.Combine(AppContext.BaseDirectory, "Migrations", "Scripts"), cancellationToken);
    }

    private async Task ExecuteSlqFilesForDirectory(string path, CancellationToken cancellationToken)
    {
        if (Directory.Exists(path))
        {
            string[] files = Directory.GetFiles(path, "*.sql", SearchOption.AllDirectories);
            foreach (string file in files.OrderBy(x => x))
            {
                using (var sr = new StreamReader(file))
                {
                    string sql = await sr.ReadToEndAsync();
                    await _db.Database.ExecuteSqlRawAsync(sql, cancellationToken);
                }
            }
        }
    }

    private async Task SetCdcEnabled(CancellationToken cancellationToken)
    {
        var tables = _db.Model.GetEntityTypes()
            .Where(x => x.ClrType.GetCustomAttribute<CdcEnabledAttribute>() != null)
            .Select(x => x.GetTableName())
            .Where(x => x != null)
            .Select(x => x!)
            .ToList();


        tables.AddRange(_db.ExtraTablesCdcEnabled());

        string? tableNames = string.Join(',', tables);

        await _db.Database.ExecuteSqlRawAsync(string.Format(@"DECLARE @CdcTables NVARCHAR(max) = '{0}'
DECLARE @TableName NVARCHAR(200)
DECLARE AddCursor CURSOR FOR
SELECT s.Value As ToAdd
FROM STRING_SPLIT(@CdcTables, ',') s
WHERE s.Value <> ''
AND EXISTS
(
        SELECT 1
        FROM sys.tables
        WHERE name = s.value
)
AND NOT EXISTS
(
	SELECT 1
	FROM sys.extended_properties
	WHERE name = 'CDCEnabled'
	AND OBJECT_NAME(major_id) = s.Value
)

OPEN AddCursor
FETCH NEXT FROM AddCursor INTO @TableName

WHILE @@FETCH_STATUS = 0
BEGIN
	EXEC sp_addextendedproperty
		@name = 'CDCEnabled',
		@value = '1',
		@level0type = N'Schema', @level0name = 'dbo',
		@level1type = N'Table',  @level1name = @TableName;

	FETCH NEXT FROM AddCursor INTO @TableName
END

CLOSE AddCursor
DEALLOCATE AddCursor

DECLARE DeleteCursor CURSOR FOR
SELECT OBJECT_NAME(e.major_id) as ToDelete
FROM sys.extended_properties e
WHERE e.name = 'CDCEnabled'
AND e.major_id <> 0
AND NOT EXISTS
(
	SELECT 1
	FROM STRING_SPLIT(@CdcTables, ',') s
	WHERE s.value = OBJECT_NAME(e.major_id)
)

OPEN DeleteCursor
FETCH NEXT FROM DeleteCursor INTO @TableName

WHILE @@FETCH_STATUS = 0
BEGIN
	EXEC sp_dropextendedproperty   
		@name = 'CDCEnabled',
		@level0type = N'Schema', @level0name = 'dbo',
		@level1type = N'Table',  @level1name = @TableName;

	FETCH NEXT FROM DeleteCursor INTO @TableName
END

CLOSE DeleteCursor
DEALLOCATE DeleteCursor", tableNames), cancellationToken);
    }


    private async Task SetObfuscateEnabled(CancellationToken cancellationToken)
    {
        var obfuscatefields = _db.Model.GetEntityTypes()
            .SelectMany(x => x.GetProperties())
            .Where(x => x.PropertyInfo?.GetCustomAttribute<ObfuscateAttribute>() != null)
            .Select(x => new { TableName = x!.DeclaringEntityType.GetTableName(), ColumnName = x.Name,ObfuscateValue = DetermineObfuscateValue(x.PropertyInfo!.GetCustomAttribute<ObfuscateAttribute>()!.ObfuscateType) })
        .ToList();

        string insertValues = string.Empty;
        if (obfuscatefields.Count() > 0)
        {
            insertValues = "insert into @columnsToObfuscate values";
            foreach (var x in obfuscatefields)
            {
                insertValues += $"('{x.TableName}','{x.ColumnName}','{x.ObfuscateValue}'),";
            }
            insertValues = insertValues.Remove(insertValues.Length - 1, 1);
        }
        string sql = string.Format(@"DECLARE @columnsToObfuscate TABLE (atable varchar(20), afield varchar(20),obfuscateValue varchar(20)) 
        {0} 
      DECLARE @tb VARCHAR(MAX), @fd VARCHAR(MAX), @ov VARCHAR(MAX)
      DECLARE AddCursor CURSOR FOR
      SELECT 
	  atable as tb
	  ,afield as fd
      ,obfuscateValue as ov
      FROM 		
		sys.columns as COL
		join sys.tables as TAB on COL.object_id = TAB.object_id
		join @columnsToObfuscate AS VARS ON TAB.name = VARS.atable AND COL.name = VARS.afield
		where not 
		exists
		(		SELECT distinct major_id, syscolumns.column_id
FROM sys.extended_properties AS EP		
inner join sys.columns as syscolumns on EP.major_id = syscolumns.object_id
WHERE EP.name = 'ObfuscateEnabled' AND 
(EP.major_id = TAB.object_id AND EP.minor_id = COL.column_id))

OPEN AddCursor;
FETCH NEXT FROM AddCursor INTO @tb, @fd, @ov
WHILE @@FETCH_STATUS = 0
    BEGIN		       
		EXEC sp_addextendedproperty		
        	@name = 'ObfuscateEnabled',
			@value = @ov,
       		@level0type = N'Schema', @level0name = 'dbo',
       		@level1type = N'Table',  @level1name = @tb,
       		@level2type = N'Column',  @level2name = @fd;
FETCH NEXT FROM AddCursor INTO @tb, @fd, @ov;
    END;
CLOSE AddCursor;
DEALLOCATE AddCursor;

DECLARE DeleteCursor CURSOR FOR
SELECT TAB.name as tb
	  ,COL.name as fd
	  FROM 	sys.extended_properties AS EP
		JOIN sys.tables as TAB on EP.major_id = TAB.object_id
		JOIN sys.columns AS COL ON EP.minor_id = COL.column_id AND EP.major_id = COL.object_id
		LEFT OUTER join @columnsToObfuscate AS VARS ON TAB.name = VARS.atable AND COL.name = VARS.afield
		WHERE EP.name = 'ObfuscateEnabled'  
		and VARS.atable IS NULL
		and VARS.afield IS NULL  
OPEN DeleteCursor;
FETCH NEXT FROM DeleteCursor INTO @tb, @fd
WHILE @@FETCH_STATUS = 0
    BEGIN		
		EXEC sp_dropextendedproperty   
		@name = 'ObfuscateEnabled',		
		@level0type = N'Schema', @level0name = 'dbo',
		@level1type = N'Table',  @level1name = @tb,		
		@level2type = N'Column',  @level2name = @fd;
       FETCH NEXT FROM DeleteCursor INTO  @tb, @fd;
    END;
CLOSE DeleteCursor
DEALLOCATE DeleteCursor", insertValues);
        await _db.Database.ExecuteSqlRawAsync(sql, cancellationToken);
    }

    private async Task SetObfuscationExceptions(CancellationToken cancellationToken)
    {
        var obfuscateExceptionTypes = _db.Model.GetEntityTypes()
            .Where(x => x.ClrType?.GetCustomAttribute<ObfuscationExceptionAttribute>() != null)
            .Select(x => x.GetTableName())
        .ToList();

        string insertValues = string.Empty;
        if (obfuscateExceptionTypes.Count() > 0)
        {
            insertValues = "insert into @tableObfuscationExceptions values";
            foreach (string? x in obfuscateExceptionTypes)
            {
                insertValues += $"('{x}'),";
            }
            insertValues = insertValues.Remove(insertValues.Length - 1, 1);
        }
        string sql = string.Format(@"DECLARE @tableObfuscationExceptions TABLE (atable varchar(20)) 
        {0} 
      DECLARE @tb VARCHAR(MAX)
      DECLARE AddCursor CURSOR FOR
      SELECT 
	  atable as tb
      FROM 		
		sys.tables as TAB
		join @tableObfuscationExceptions AS VARS ON TAB.name = VARS.atable
		where not 
		exists
		(		SELECT distinct major_id
FROM sys.extended_properties AS EP		
WHERE EP.name = 'ObfuscationException' AND 
EP.major_id = TAB.object_id)

OPEN AddCursor;
FETCH NEXT FROM AddCursor INTO @tb
WHILE @@FETCH_STATUS = 0
    BEGIN		       
		EXEC sp_addextendedproperty		
        	@name = 'ObfuscationException',
			@value = '1',
       		@level0type = N'Schema', @level0name = 'dbo',
       		@level1type = N'Table',  @level1name = @tb;
FETCH NEXT FROM AddCursor INTO @tb;
    END;
CLOSE AddCursor;
DEALLOCATE AddCursor;

DECLARE DeleteCursor CURSOR FOR
SELECT TAB.name as tb
	  FROM 	sys.extended_properties AS EP
		JOIN sys.tables as TAB on EP.major_id = TAB.object_id
		LEFT OUTER join @tableObfuscationExceptions AS VARS ON TAB.name = VARS.atable
		WHERE EP.name = 'ObfuscationException'  
		and VARS.atable IS NULL
OPEN DeleteCursor;
FETCH NEXT FROM DeleteCursor INTO @tb
WHILE @@FETCH_STATUS = 0
    BEGIN		
		EXEC sp_dropextendedproperty   
		@name = 'ObfuscationException',		
		@level0type = N'Schema', @level0name = 'dbo',
		@level1type = N'Table',  @level1name = @tb;
       FETCH NEXT FROM DeleteCursor INTO  @tb;
    END;
CLOSE DeleteCursor
DEALLOCATE DeleteCursor", insertValues);
        await _db.Database.ExecuteSqlRawAsync(sql, cancellationToken);
    }

    private string DetermineObfuscateValue(ObfuscateType obfuscateType)
    {
        switch (obfuscateType)
        {   
            case ObfuscateType.String:
                return "STRING";
            case ObfuscateType.SeparatedString:
                return "SEPARATEDSTRING";
            case ObfuscateType.SearchItemString:
                return "SEARCHITEMSTRING";
            case ObfuscateType.Photo:
                return "PHOTO";
            default:
                throw new NotSupportedException($"{obfuscateType} is not a known type of obfuscation.");
        }
    }
}
