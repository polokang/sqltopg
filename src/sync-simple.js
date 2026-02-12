import dotenv from 'dotenv';
import sql from 'mssql';
import { PrismaClient } from '@prisma/client';
import { fileURLToPath } from 'url';
import { resolve, dirname, join } from 'path';
import { readFileSync } from 'fs';

dotenv.config();
const prisma = new PrismaClient();

// 解析 SQL Server 连接字符串
function parseSQLServerConnectionString(connectionString) {
  let cleanString = connectionString.replace(/^["']?sqlserver:\/\/["']?/i, '').trim();
  const config = {};
  
  const hostMatch = cleanString.match(/^([^;:]+):?(\d+)?/);
  if (hostMatch) {
    config.server = hostMatch[1];
    if (hostMatch[2]) {
      config.port = parseInt(hostMatch[2]);
    }
    cleanString = cleanString.substring(hostMatch[0].length).replace(/^;/, '');
  }
  
  const params = cleanString.split(';').filter(Boolean);
  params.forEach(param => {
    const equalIndex = param.indexOf('=');
    if (equalIndex > 0) {
      const key = param.substring(0, equalIndex).trim();
      const value = param.substring(equalIndex + 1).trim();
      const lowerKey = key.toLowerCase();
      
      switch (lowerKey) {
        case 'database': config.database = value; break;
        case 'user': config.user = value; break;
        case 'password': config.password = value; break;
        case 'trustservercertificate':
          config.options = config.options || {};
          config.options.trustServerCertificate = value === 'true';
          break;
      }
    }
  });
  
  config.options = { encrypt: true, trustServerCertificate: true, enableArithAbort: true, ...config.options };
  config.pool = { max: 10, min: 0, idleTimeoutMillis: 30000 };
  
  return config;
}

const sqlServerConfig = parseSQLServerConnectionString(process.env.SQL_SERVER_URL || '');

// 辅助函数：转义 SQL 字符串
function escapeSQL(value) {
  if (value === null || value === undefined) return 'NULL';
  if (typeof value === 'boolean') return value ? 'true' : 'false';
  if (typeof value === 'number') return value.toString();
  // 转义单引号并添加引号
  return `'${value.toString().replace(/'/g, "''")}'`;
}

// 转换布尔值
function toBoolean(value) {
  return value === true || value === 1 || value === '1' ? 'true' : 'false';
}

// 转换日期时间
function toTimestamp(value) {
  if (!value) return 'NULL';
  if (value instanceof Date) {
    return `'${value.toISOString()}'`;
  }
  return `'${value}'`;
}

// 检查表是否为空（用于判断是否需要全量同步）
async function isTableEmpty(tableName) {
  try {
    const result = await prisma.$queryRawUnsafe(`SELECT COUNT(*) as count FROM ${tableName}`);
    return result[0]?.count === 0 || result[0]?.count === '0';
  } catch (error) {
    // 如果表不存在，返回 true（需要全量同步）
    return true;
  }
}

// 记录同步变更日志
function logChange(type, tableName, record, keyField, keyValue) {
  const timestamp = new Date().toISOString();
  const logMessage = `[${timestamp}] [${type}] ${tableName} - ${keyField}=${keyValue}`;
  console.log(`  📝 ${logMessage}`);
  
  // 如果是更新，记录变更的字段
  if (type === 'UPDATE' && record) {
    const changedFields = Object.keys(record).filter(k => k !== keyField);
    if (changedFields.length > 0) {
      console.log(`     变更字段: ${changedFields.join(', ')}`);
    }
  }
}

// 同步 users 表
async function syncUsers(pool, isFullSync = false) {
  console.log('\n=== 同步 users 表 ===');
  
  // 检查是否需要全量同步
  const isEmpty = await isTableEmpty('users');
  const needFullSync = isFullSync || isEmpty;
  
  if (needFullSync) {
    console.log('  模式: 全量同步（表为空或强制全量）');
  } else {
    console.log('  模式: 增量同步');
  }
  
  const result = await pool.request().query(`
    SELECT 
      UserId, CompanyId, UserTypeId, Mobile, Email, 
      COALESCE(Name, UserName) AS Name, PasswordHash, Activated,
      DateCreated, COALESCE(LastUpdateDate, DateCreated) AS LastUpdateDate
    FROM UserInfomation
    ORDER BY Email, Activated DESC, CASE WHEN UserTypeId = 24 THEN 0 ELSE 1 END, UserId DESC
  `);
  
  console.log(`读取到 ${result.recordset.length} 条用户记录`);
  
  if (result.recordset.length === 0) {
    console.log('没有用户数据需要同步');
    return { validUserIds: new Set() };
  }
  
  // 处理重复的 Email：优先级规则
  // 1. Activated=1 优先
  // 2. UserTypeId=24 优先
  // 3. UserId 大的优先
  const emailMap = new Map();
  const recordsToSync = [];
  const validUserIds = new Set(); // 保存有效的 UserId，供其他表同步使用
  let duplicateCount = 0;
  
  // 比较函数：判断新记录是否应该替换已有记录
  function shouldReplace(existing, current) {
    // 优先级1: Activated=1 优先
    const existingActivated = existing.Activated === 1 || existing.Activated === true || existing.Activated === '1';
    const currentActivated = current.Activated === 1 || current.Activated === true || current.Activated === '1';
    
    if (currentActivated && !existingActivated) return true;
    if (!currentActivated && existingActivated) return false;
    
    // 优先级2: UserTypeId=24 优先
    const existingIsType24 = existing.UserTypeId === 24;
    const currentIsType24 = current.UserTypeId === 24;
    
    if (currentIsType24 && !existingIsType24) return true;
    if (!currentIsType24 && existingIsType24) return false;
    
    // 优先级3: UserId 大的优先
    return current.UserId > existing.UserId;
  }
  
  for (const record of result.recordset) {
    const email = record.Email?.toLowerCase().trim();
    if (!email) {
      // 如果没有 Email，直接添加（虽然不应该发生，因为 Email 是 NOT NULL）
      recordsToSync.push(record);
      validUserIds.add(record.UserId);
      continue;
    }
    
    if (emailMap.has(email)) {
      const existingRecord = emailMap.get(email);
      // 根据优先级规则判断是否替换
      if (shouldReplace(existingRecord, record)) {
        duplicateCount++;
        emailMap.set(email, record);
        // 从已添加的记录中移除旧的
        const index = recordsToSync.findIndex(r => r.UserId === existingRecord.UserId);
        if (index !== -1) {
          recordsToSync.splice(index, 1);
          validUserIds.delete(existingRecord.UserId);
        }
        recordsToSync.push(record);
        validUserIds.add(record.UserId);
        console.log(`  发现重复 Email "${email}": 保留 UserId=${record.UserId} (Activated=${record.Activated}, UserTypeId=${record.UserTypeId})，移除 UserId=${existingRecord.UserId} (Activated=${existingRecord.Activated}, UserTypeId=${existingRecord.UserTypeId})`);
      } else {
        duplicateCount++;
        console.log(`  发现重复 Email "${email}": 保留 UserId=${existingRecord.UserId} (Activated=${existingRecord.Activated}, UserTypeId=${existingRecord.UserTypeId})，跳过 UserId=${record.UserId} (Activated=${record.Activated}, UserTypeId=${record.UserTypeId})`);
      }
    } else {
      emailMap.set(email, record);
      recordsToSync.push(record);
      validUserIds.add(record.UserId);
    }
  }
  
  if (duplicateCount > 0) {
    console.log(`  处理了 ${duplicateCount} 条重复 Email 的记录，最终同步 ${recordsToSync.length} 条记录`);
  }
  
  let inserted = 0;
  let updated = 0;
  let deleted = 0;
  
  if (needFullSync) {
    // 全量同步：清空表后插入所有记录
    await prisma.$executeRawUnsafe('TRUNCATE TABLE users CASCADE');
    console.log('✓ 已清空 users 表');
    
    const batchSize = 500;
    for (let i = 0; i < recordsToSync.length; i += batchSize) {
      const batch = recordsToSync.slice(i, i + batchSize);
      
      const values = batch.map(record => {
        const mobile = record.Mobile && record.Mobile.trim() !== '' 
          ? escapeSQL(record.Mobile) 
          : "'0000000000'";
        
        return `(
          ${escapeSQL(record.UserId)},
          ${escapeSQL(record.CompanyId)},
          ${escapeSQL(record.UserTypeId)},
          ${mobile},
          ${escapeSQL(record.Email)},
          ${escapeSQL(record.Name)},
          ${escapeSQL(record.PasswordHash)},
          ${toBoolean(record.Activated)},
          ${toTimestamp(record.DateCreated)},
          ${toTimestamp(record.LastUpdateDate)}
        )`;
      }).join(', ');
      
      const query = `
        INSERT INTO users (
          user_id, company_id, user_type_id, mobile, email, name, password_hash, activated, created_at, updated_at
        ) VALUES ${values}
        ON CONFLICT (user_id) DO UPDATE SET
          company_id = EXCLUDED.company_id,
          user_type_id = EXCLUDED.user_type_id,
          mobile = EXCLUDED.mobile,
          email = EXCLUDED.email,
          name = EXCLUDED.name,
          password_hash = EXCLUDED.password_hash,
          activated = EXCLUDED.activated,
          updated_at = EXCLUDED.updated_at
      `;
      
      await prisma.$executeRawUnsafe(query);
      inserted += batch.length;
      console.log(`  已插入 ${inserted}/${recordsToSync.length} 条用户记录...`);
    }
    
    console.log(`✓ users 表全量同步完成，共 ${inserted} 条记录`);
  } else {
    // 增量同步：比较差异，只同步变化的记录
    console.log('  开始增量同步...');
    
    // 从 PostgreSQL 读取现有记录
    const existingRecords = await prisma.$queryRawUnsafe(`
      SELECT user_id, company_id, user_type_id, mobile, email, name, password_hash, activated, created_at, updated_at
      FROM users
    `);
    
    const existingMap = new Map();
    existingRecords.forEach(record => {
      existingMap.set(String(record.user_id), record);
    });
    
    const sourceMap = new Map();
    recordsToSync.forEach(record => {
      sourceMap.set(String(record.UserId), record);
    });
    
    // 找出需要新增和更新的记录
    const toInsert = [];
    const toUpdate = [];
    
    for (const record of recordsToSync) {
      const userId = String(record.UserId);
      const existing = existingMap.get(userId);
      
      if (!existing) {
        // 新增记录
        toInsert.push(record);
        logChange('INSERT', 'users', record, 'user_id', record.UserId);
      } else {
        // 检查是否有变化（比较所有字段，确保类型一致）
        const mobile = record.Mobile && record.Mobile.trim() !== '' ? record.Mobile : '0000000000';
        const activatedValue = record.Activated === 1 || record.Activated === true || record.Activated === '1' || record.Activated === 'true';
        
        // 规范化比较值（确保类型一致）
        const normalizeValue = (val) => {
          if (val === null || val === undefined) return null;
          if (typeof val === 'boolean') return val;
          if (typeof val === 'number') return val;
          return String(val);
        };
        
        const hasChanged = 
          normalizeValue(existing.company_id) !== normalizeValue(record.CompanyId) ||
          normalizeValue(existing.user_type_id) !== normalizeValue(record.UserTypeId) ||
          normalizeValue(existing.mobile) !== normalizeValue(mobile) ||
          normalizeValue(existing.email) !== normalizeValue(record.Email) ||
          normalizeValue(existing.name) !== normalizeValue(record.Name) ||
          normalizeValue(existing.password_hash) !== normalizeValue(record.PasswordHash) ||
          Boolean(existing.activated) !== Boolean(activatedValue);
        
        if (hasChanged) {
          toUpdate.push(record);
          logChange('UPDATE', 'users', record, 'user_id', record.UserId);
        }
      }
    }
    
    // 找出需要删除的记录（在目标中存在但源中不存在）
    const toDelete = [];
    for (const [userId, existing] of existingMap.entries()) {
      if (!sourceMap.has(userId)) {
        toDelete.push(userId);
        logChange('DELETE', 'users', null, 'user_id', userId);
      }
    }
    
    // 执行新增
    if (toInsert.length > 0) {
      const batchSize = 500;
      for (let i = 0; i < toInsert.length; i += batchSize) {
        const batch = toInsert.slice(i, i + batchSize);
        const values = batch.map(record => {
          const mobile = record.Mobile && record.Mobile.trim() !== '' 
            ? escapeSQL(record.Mobile) 
            : "'0000000000'";
          
          return `(
            ${escapeSQL(record.UserId)},
            ${escapeSQL(record.CompanyId)},
            ${escapeSQL(record.UserTypeId)},
            ${mobile},
            ${escapeSQL(record.Email)},
            ${escapeSQL(record.Name)},
            ${escapeSQL(record.PasswordHash)},
            ${toBoolean(record.Activated)},
            ${toTimestamp(record.DateCreated)},
            ${toTimestamp(record.LastUpdateDate)}
          )`;
        }).join(', ');
        
        const query = `
          INSERT INTO users (
            user_id, company_id, user_type_id, mobile, email, name, password_hash, activated, created_at, updated_at
          ) VALUES ${values}
        `;
        
        await prisma.$executeRawUnsafe(query);
        inserted += batch.length;
      }
    }
    
    // 执行更新
    if (toUpdate.length > 0) {
      const batchSize = 500;
      for (let i = 0; i < toUpdate.length; i += batchSize) {
        const batch = toUpdate.slice(i, i + batchSize);
        const values = batch.map(record => {
          const mobile = record.Mobile && record.Mobile.trim() !== '' 
            ? escapeSQL(record.Mobile) 
            : "'0000000000'";
          
          return `(
            ${escapeSQL(record.UserId)},
            ${escapeSQL(record.CompanyId)},
            ${escapeSQL(record.UserTypeId)},
            ${mobile},
            ${escapeSQL(record.Email)},
            ${escapeSQL(record.Name)},
            ${escapeSQL(record.PasswordHash)},
            ${toBoolean(record.Activated)},
            ${toTimestamp(record.DateCreated)},
            ${toTimestamp(record.LastUpdateDate)}
          )`;
        }).join(', ');
        
        const query = `
          INSERT INTO users (
            user_id, company_id, user_type_id, mobile, email, name, password_hash, activated, created_at, updated_at
          ) VALUES ${values}
          ON CONFLICT (user_id) DO UPDATE SET
            company_id = EXCLUDED.company_id,
            user_type_id = EXCLUDED.user_type_id,
            mobile = EXCLUDED.mobile,
            email = EXCLUDED.email,
            name = EXCLUDED.name,
            password_hash = EXCLUDED.password_hash,
            activated = EXCLUDED.activated,
            updated_at = EXCLUDED.updated_at
        `;
        
        await prisma.$executeRawUnsafe(query);
        updated += batch.length;
      }
    }
    
    // 执行删除
    if (toDelete.length > 0) {
      const deleteQuery = `DELETE FROM users WHERE user_id IN (${toDelete.map(id => escapeSQL(id)).join(', ')})`;
      await prisma.$executeRawUnsafe(deleteQuery);
      deleted = toDelete.length;
    }
    
    console.log(`✓ users 表增量同步完成: 新增 ${inserted} 条, 更新 ${updated} 条, 删除 ${deleted} 条`);
  }
  
  // 返回有效的 UserId 集合，供其他表同步使用
  return { validUserIds };
}

// 同步 user_profiles 表
async function syncUserProfiles(pool, validUserIds, isFullSync = false) {
  console.log('\n=== 同步 user_profiles 表 ===');
  
  // 检查是否需要全量同步
  const isEmpty = await isTableEmpty('user_profiles');
  const needFullSync = isFullSync || isEmpty;
  
  if (needFullSync) {
    console.log('  模式: 全量同步（表为空或强制全量）');
  } else {
    console.log('  模式: 增量同步');
  }
  
  const result = await pool.request().query(`
    SELECT 
      UserId,
      LastLoginTime,
      LastLogoutTime,
      IsRead,
      IsPhoneVerified,
      RequiresTwoFactorAuth,
      Logo,
      Production,
      DateCreated,
      COALESCE(LastUpdateDate, DateCreated) AS LastUpdateDate
    FROM UserInfomation
  `);
  
  console.log(`读取到 ${result.recordset.length} 条用户扩展信息记录`);
  
  if (result.recordset.length === 0) {
    console.log('没有用户扩展信息需要同步');
    return;
  }
  
  // 过滤掉无效的 UserId（在去重过程中被移除的）
  const recordsToSync = result.recordset.filter(record => validUserIds.has(record.UserId));
  
  if (recordsToSync.length < result.recordset.length) {
    const filteredCount = result.recordset.length - recordsToSync.length;
    console.log(`  过滤了 ${filteredCount} 条无效记录（对应的 UserId 在 users 表中不存在）`);
  }
  
  if (recordsToSync.length === 0) {
    console.log('没有有效的用户扩展信息需要同步');
    return;
  }
  
  let inserted = 0;
  let updated = 0;
  let deleted = 0;
  
  if (needFullSync) {
    // 全量同步
    await prisma.$executeRawUnsafe('TRUNCATE TABLE user_profiles CASCADE');
    console.log('✓ 已清空 user_profiles 表');
    
    const batchSize = 500;
    for (let i = 0; i < recordsToSync.length; i += batchSize) {
      const batch = recordsToSync.slice(i, i + batchSize);
      const values = batch.map(record => {
        return `(
          ${escapeSQL(record.UserId)},
          ${toTimestamp(record.LastLoginTime)},
          ${toTimestamp(record.LastLogoutTime)},
          ${toBoolean(record.IsRead)},
          ${toBoolean(record.IsPhoneVerified)},
          ${toBoolean(record.RequiresTwoFactorAuth)},
          ${escapeSQL(record.Logo)},
          ${toBoolean(record.Production)},
          ${toTimestamp(record.DateCreated)},
          ${toTimestamp(record.LastUpdateDate)}
        )`;
      }).join(', ');
      
      const query = `INSERT INTO user_profiles (
        user_id, last_login_time, last_logout_time, is_read, is_phone_verified,
        requires_two_factor_auth, logo, production, created_at, updated_at
      ) VALUES ${values}`;
      
      await prisma.$executeRawUnsafe(query);
      inserted += batch.length;
      console.log(`  已插入 ${inserted}/${recordsToSync.length} 条用户扩展信息记录...`);
    }
    console.log(`✓ user_profiles 表全量同步完成，共 ${inserted} 条记录`);
  } else {
    // 增量同步
    console.log('  开始增量同步...');
    
    // 读取现有记录的所有字段用于比较
    const existingRecords = await prisma.$queryRawUnsafe(`
      SELECT user_id, last_login_time, last_logout_time, is_read, is_phone_verified,
             requires_two_factor_auth, logo, production, created_at, updated_at
      FROM user_profiles
    `);
    
    const existingMap = new Map();
    existingRecords.forEach(r => {
      existingMap.set(String(r.user_id), r);
    });
    
    const sourceUserIds = new Set(recordsToSync.map(r => String(r.UserId)));
    
    const toInsert = [];
    const toUpdate = [];
    
    // 辅助函数：规范化值用于比较
    const normalizeValue = (val) => {
      if (val === null || val === undefined) return null;
      if (val instanceof Date) return val.toISOString();
      if (typeof val === 'boolean') return val;
      return String(val);
    };
    
    // 辅助函数：比较日期时间
    const compareTimestamp = (val1, val2) => {
      if (!val1 && !val2) return true;
      if (!val1 || !val2) return false;
      const d1 = val1 instanceof Date ? val1 : new Date(val1);
      const d2 = val2 instanceof Date ? val2 : new Date(val2);
      return d1.getTime() === d2.getTime();
    };
    
    for (const record of recordsToSync) {
      const userId = String(record.UserId);
      const existing = existingMap.get(userId);
      
      if (!existing) {
        toInsert.push(record);
        logChange('INSERT', 'user_profiles', record, 'user_id', userId);
      } else {
        // 比较所有字段
        const hasChanged = 
          !compareTimestamp(existing.last_login_time, record.LastLoginTime) ||
          !compareTimestamp(existing.last_logout_time, record.LastLogoutTime) ||
          Boolean(existing.is_read) !== Boolean(record.IsRead === 1 || record.IsRead === true || record.IsRead === '1') ||
          Boolean(existing.is_phone_verified) !== Boolean(record.IsPhoneVerified === 1 || record.IsPhoneVerified === true || record.IsPhoneVerified === '1') ||
          Boolean(existing.requires_two_factor_auth) !== Boolean(record.RequiresTwoFactorAuth === 1 || record.RequiresTwoFactorAuth === true || record.RequiresTwoFactorAuth === '1') ||
          normalizeValue(existing.logo) !== normalizeValue(record.Logo) ||
          Boolean(existing.production) !== Boolean(record.Production === 1 || record.Production === true || record.Production === '1');
        
        if (hasChanged) {
          toUpdate.push(record);
          logChange('UPDATE', 'user_profiles', record, 'user_id', userId);
        }
      }
    }
    
    // 找出需要删除的记录
    for (const [userId] of existingMap.entries()) {
      if (!sourceUserIds.has(userId)) {
        deleted++;
        logChange('DELETE', 'user_profiles', null, 'user_id', userId);
      }
    }
    
    // 执行新增
    if (toInsert.length > 0) {
      const batchSize = 500;
      for (let i = 0; i < toInsert.length; i += batchSize) {
        const batch = toInsert.slice(i, i + batchSize);
        const values = batch.map(record => {
          return `(
            ${escapeSQL(record.UserId)},
            ${toTimestamp(record.LastLoginTime)},
            ${toTimestamp(record.LastLogoutTime)},
            ${toBoolean(record.IsRead)},
            ${toBoolean(record.IsPhoneVerified)},
            ${toBoolean(record.RequiresTwoFactorAuth)},
            ${escapeSQL(record.Logo)},
            ${toBoolean(record.Production)},
            ${toTimestamp(record.DateCreated)},
            ${toTimestamp(record.LastUpdateDate)}
          )`;
        }).join(', ');
        
        const query = `INSERT INTO user_profiles (
          user_id, last_login_time, last_logout_time, is_read, is_phone_verified,
          requires_two_factor_auth, logo, production, created_at, updated_at
        ) VALUES ${values}`;
        
        await prisma.$executeRawUnsafe(query);
        inserted += batch.length;
      }
    }
    
    // 执行更新（只有真正有变化的记录）
    if (toUpdate.length > 0) {
      const batchSize = 500;
      for (let i = 0; i < toUpdate.length; i += batchSize) {
        const batch = toUpdate.slice(i, i + batchSize);
        const values = batch.map(record => {
          return `(
            ${escapeSQL(record.UserId)},
            ${toTimestamp(record.LastLoginTime)},
            ${toTimestamp(record.LastLogoutTime)},
            ${toBoolean(record.IsRead)},
            ${toBoolean(record.IsPhoneVerified)},
            ${toBoolean(record.RequiresTwoFactorAuth)},
            ${escapeSQL(record.Logo)},
            ${toBoolean(record.Production)},
            ${toTimestamp(record.DateCreated)},
            ${toTimestamp(record.LastUpdateDate)}
          )`;
        }).join(', ');
        
        const query = `INSERT INTO user_profiles (
          user_id, last_login_time, last_logout_time, is_read, is_phone_verified,
          requires_two_factor_auth, logo, production, created_at, updated_at
        ) VALUES ${values}
        ON CONFLICT (user_id) DO UPDATE SET
          last_login_time = EXCLUDED.last_login_time,
          last_logout_time = EXCLUDED.last_logout_time,
          is_read = EXCLUDED.is_read,
          is_phone_verified = EXCLUDED.is_phone_verified,
          requires_two_factor_auth = EXCLUDED.requires_two_factor_auth,
          logo = EXCLUDED.logo,
          production = EXCLUDED.production,
          updated_at = EXCLUDED.updated_at`;
        
        await prisma.$executeRawUnsafe(query);
        updated += batch.length;
      }
    }
    
    // 执行删除
    if (deleted > 0) {
      const toDeleteIds = Array.from(existingMap.keys()).filter(id => !sourceUserIds.has(id));
      const deleteQuery = `DELETE FROM user_profiles WHERE user_id IN (${toDeleteIds.map(id => escapeSQL(id)).join(', ')})`;
      await prisma.$executeRawUnsafe(deleteQuery);
    }
    
    console.log(`✓ user_profiles 表增量同步完成: 新增 ${inserted} 条, 更新 ${updated} 条, 删除 ${deleted} 条`);
  }
}

// 同步 controllers 表
async function syncControllers(pool, validUserIds, isFullSync = false) {
  console.log('\n=== 同步 controllers 表 ===');
  
  const isEmpty = await isTableEmpty('controllers');
  const needFullSync = isFullSync || isEmpty;
  
  if (needFullSync) {
    console.log('  模式: 全量同步（表为空或强制全量）');
  } else {
    console.log('  模式: 增量同步');
  }
  
  const result = await pool.request().query(`
    SELECT 
      UnitId, SIMCardNo, SystemID, FirmwareVersion, Activated,
      ControllerModel AS ModelType, DateCreated, DateLastUpdate,
      SiteLocation AS Creator, Suburb, TimeZoneId, AccManagerId
    FROM ControllerInfo
  `);
  
  // 处理 Suburb 字段：将逗号分割的字符串转换为数组
  result.recordset.forEach(record => {
    if (record.Suburb) {
      // 将逗号分割的字符串转换为数组，并去除每个元素的前后空格
      record.tags = record.Suburb.split(',')
        .map(tag => tag.trim())
        .filter(tag => tag.length > 0); // 过滤空字符串
    } else {
      record.tags = []; // 如果没有 Suburb，设置为空数组
    }
  });
  
  console.log(`读取到 ${result.recordset.length} 条控制器记录`);
  
  if (result.recordset.length === 0) {
    console.log('没有控制器数据需要同步');
    return;
  }
  
  let inserted = 0;
  let updated = 0;
  let deleted = 0;
  
  if (needFullSync) {
    // 全量同步
    await prisma.$executeRawUnsafe('TRUNCATE TABLE controllers CASCADE');
    console.log('✓ 已清空 controllers 表');
  } else {
    // 增量同步：比较差异，只同步变化的记录
    console.log('  开始增量同步...');
    
    // 读取现有记录的所有字段用于比较
    const existingRecords = await prisma.$queryRawUnsafe(`
      SELECT unit_id, sim_card_no, billing_user_id, system_id, firmware_version,
             activated, model_type, date_created, date_last_updated,
             creator, tags, timezone_id
      FROM controllers
    `);
    
    const existingMap = new Map();
    existingRecords.forEach(r => {
      // 规范化 tags 数组用于比较
      const tagsArray = Array.isArray(r.tags) ? r.tags.sort() : [];
      existingMap.set(String(r.unit_id), {
        ...r,
        tagsArray: tagsArray
      });
    });
    
    const sourceMap = new Map();
    result.recordset.forEach(r => {
      sourceMap.set(String(r.UnitId), r);
    });
    
    // 辅助函数：比较数组
    const compareArrays = (arr1, arr2) => {
      if (!arr1 && !arr2) return true;
      if (!arr1 || !arr2) return false;
      const a1 = Array.isArray(arr1) ? [...arr1].sort() : [];
      const a2 = Array.isArray(arr2) ? [...arr2].sort() : [];
      if (a1.length !== a2.length) return false;
      return a1.every((val, idx) => val === a2[idx]);
    };
    
    // 辅助函数：规范化值
    const normalizeValue = (val) => {
      if (val === null || val === undefined) return null;
      if (typeof val === 'boolean') return val;
      if (typeof val === 'number') return val;
      return String(val);
    };
    
    // 找出需要新增、更新和删除的记录
    const toInsert = [];
    const toUpdate = [];
    
    for (const record of result.recordset) {
      const unitId = String(record.UnitId);
      const existing = existingMap.get(unitId);
      
      if (!existing) {
        toInsert.push(record);
        logChange('INSERT', 'controllers', record, 'unit_id', unitId);
      } else {
        // 准备比较用的值
        const recordTags = record.tags || [];
        const recordBillingUserId = record.AccManagerId && validUserIds.has(record.AccManagerId) ? record.AccManagerId : null;
        const recordActivated = record.Activated === 1 || record.Activated === true || record.Activated === '1';
        
        // 比较所有字段
        const hasChanged = 
          normalizeValue(existing.sim_card_no) !== normalizeValue(record.SIMCardNo) ||
          normalizeValue(existing.billing_user_id) !== normalizeValue(recordBillingUserId) ||
          normalizeValue(existing.system_id) !== normalizeValue(record.SystemID) ||
          normalizeValue(existing.firmware_version) !== normalizeValue(record.FirmwareVersion) ||
          Boolean(existing.activated) !== Boolean(recordActivated) ||
          normalizeValue(existing.model_type) !== normalizeValue(record.ModelType) ||
          normalizeValue(existing.creator) !== normalizeValue(record.Creator) ||
          normalizeValue(existing.timezone_id) !== normalizeValue(record.TimeZoneId) ||
          !compareArrays(existing.tagsArray, recordTags);
        
        if (hasChanged) {
          toUpdate.push(record);
          logChange('UPDATE', 'controllers', record, 'unit_id', unitId);
        }
      }
    }
    
    // 找出需要删除的记录
    for (const [unitId] of existingMap.entries()) {
      if (!sourceMap.has(unitId)) {
        deleted++;
        logChange('DELETE', 'controllers', null, 'unit_id', unitId);
      }
    }
    
    // 执行删除
    if (deleted > 0) {
      const toDeleteIds = Array.from(existingMap.keys()).filter(id => !sourceMap.has(id));
      const deleteQuery = `DELETE FROM controllers WHERE unit_id IN (${toDeleteIds.map(id => escapeSQL(id)).join(', ')})`;
      await prisma.$executeRawUnsafe(deleteQuery);
    }
    
    // 执行新增
    if (toInsert.length > 0) {
      const batchSize = 500;
      for (let i = 0; i < toInsert.length; i += batchSize) {
        const batch = toInsert.slice(i, i + batchSize);
        const values = batch.map(record => {
          let tagsValue = 'ARRAY[]::TEXT[]';
          if (record.tags && record.tags.length > 0) {
            const tagsEscaped = record.tags.map(tag => escapeSQL(tag));
            tagsValue = `ARRAY[${tagsEscaped.join(', ')}]`;
          }
          
          let billingUserId = 'NULL';
          if (record.AccManagerId && validUserIds.has(record.AccManagerId)) {
            billingUserId = escapeSQL(record.AccManagerId);
          }
          
          return `(
            ${escapeSQL(record.UnitId)},
            ${escapeSQL(record.SIMCardNo)},
            ${billingUserId},
            ${escapeSQL(record.SystemID)},
            ${escapeSQL(record.FirmwareVersion)},
            ${toBoolean(record.Activated)},
            ${escapeSQL(record.ModelType)},
            ${toTimestamp(record.DateCreated)},
            ${toTimestamp(record.DateLastUpdate || record.DateCreated)},
            ${escapeSQL(record.Creator)},
            ${tagsValue},
            ${escapeSQL(record.TimeZoneId)}
          )`;
        }).join(', ');
        
        const query = `INSERT INTO controllers (
          unit_id, sim_card_no, billing_user_id, system_id, firmware_version,
          activated, model_type, date_created, date_last_updated,
          creator, tags, timezone_id
        ) VALUES ${values}`;
        
        await prisma.$executeRawUnsafe(query);
        inserted += batch.length;
      }
    }
    
    // 执行更新（只有真正有变化的记录）
    if (toUpdate.length > 0) {
      const batchSize = 500;
      for (let i = 0; i < toUpdate.length; i += batchSize) {
        const batch = toUpdate.slice(i, i + batchSize);
        const values = batch.map(record => {
          let tagsValue = 'ARRAY[]::TEXT[]';
          if (record.tags && record.tags.length > 0) {
            const tagsEscaped = record.tags.map(tag => escapeSQL(tag));
            tagsValue = `ARRAY[${tagsEscaped.join(', ')}]`;
          }
          
          let billingUserId = 'NULL';
          if (record.AccManagerId && validUserIds.has(record.AccManagerId)) {
            billingUserId = escapeSQL(record.AccManagerId);
          }
          
          return `(
            ${escapeSQL(record.UnitId)},
            ${escapeSQL(record.SIMCardNo)},
            ${billingUserId},
            ${escapeSQL(record.SystemID)},
            ${escapeSQL(record.FirmwareVersion)},
            ${toBoolean(record.Activated)},
            ${escapeSQL(record.ModelType)},
            ${toTimestamp(record.DateCreated)},
            ${toTimestamp(record.DateLastUpdate || record.DateCreated)},
            ${escapeSQL(record.Creator)},
            ${tagsValue},
            ${escapeSQL(record.TimeZoneId)}
          )`;
        }).join(', ');
        
        const query = `INSERT INTO controllers (
          unit_id, sim_card_no, billing_user_id, system_id, firmware_version,
          activated, model_type, date_created, date_last_updated,
          creator, tags, timezone_id
        ) VALUES ${values}
        ON CONFLICT (unit_id) DO UPDATE SET
          sim_card_no = EXCLUDED.sim_card_no,
          billing_user_id = EXCLUDED.billing_user_id,
          system_id = EXCLUDED.system_id,
          firmware_version = EXCLUDED.firmware_version,
          activated = EXCLUDED.activated,
          model_type = EXCLUDED.model_type,
          date_last_updated = EXCLUDED.date_last_updated,
          creator = EXCLUDED.creator,
          tags = EXCLUDED.tags,
          timezone_id = EXCLUDED.timezone_id`;
        
        await prisma.$executeRawUnsafe(query);
        updated += batch.length;
      }
    }
    
    console.log(`✓ controllers 表增量同步完成: 新增 ${inserted} 条, 更新 ${updated} 条, 删除 ${deleted} 条`);
    return;
  }
  
  // 全量同步：批量插入
  const batchSize = 500;
  for (let i = 0; i < result.recordset.length; i += batchSize) {
    const batch = result.recordset.slice(i, i + batchSize);
    
    const values = batch.map(record => {
      // 将 tags 数组转换为 PostgreSQL 数组格式
      let tagsValue = 'ARRAY[]::TEXT[]';
      if (record.tags && record.tags.length > 0) {
        const tagsEscaped = record.tags.map(tag => escapeSQL(tag));
        tagsValue = `ARRAY[${tagsEscaped.join(', ')}]`;
      }
      
      // billing_user_id 直接从 AccManagerId 获取，但需要验证该用户是否存在
      let billingUserId = 'NULL';
      if (record.AccManagerId && validUserIds.has(record.AccManagerId)) {
        billingUserId = escapeSQL(record.AccManagerId);
      } else if (record.AccManagerId) {
        // 如果 AccManagerId 不在有效用户列表中，记录警告但继续处理
        console.log(`  ⚠ 警告: AccManagerId=${record.AccManagerId} 在 users 表中不存在，billing_user_id 设置为 NULL`);
      }
      
      return `(
        ${escapeSQL(record.UnitId)},
        ${escapeSQL(record.SIMCardNo)},
        ${billingUserId},
        ${escapeSQL(record.SystemID)},
        ${escapeSQL(record.FirmwareVersion)},
        ${toBoolean(record.Activated)},
        ${escapeSQL(record.ModelType)},
        ${toTimestamp(record.DateCreated)},
        ${toTimestamp(record.DateLastUpdate || record.DateCreated)},
        ${escapeSQL(record.Creator)},
        ${tagsValue},
        ${escapeSQL(record.TimeZoneId)}
      )`;
    }).join(', ');
    
    const query = `
      INSERT INTO controllers (
        unit_id, sim_card_no, billing_user_id, system_id, firmware_version,
        activated, model_type, date_created, date_last_updated,
        creator, tags, timezone_id
      ) VALUES ${values}
      ON CONFLICT (unit_id) DO UPDATE SET
        sim_card_no = EXCLUDED.sim_card_no,
        billing_user_id = EXCLUDED.billing_user_id,
        system_id = EXCLUDED.system_id,
        firmware_version = EXCLUDED.firmware_version,
        activated = EXCLUDED.activated,
        model_type = EXCLUDED.model_type,
        date_last_updated = EXCLUDED.date_last_updated,
        creator = EXCLUDED.creator,
        tags = EXCLUDED.tags,
        timezone_id = EXCLUDED.timezone_id
    `;
    
    await prisma.$executeRawUnsafe(query);
    inserted += batch.length;
    console.log(`  已插入 ${inserted}/${result.recordset.length} 条控制器记录...`);
  }
  
  console.log(`✓ controllers 表全量同步完成，共 ${inserted} 条记录`);
}

// 同步 controller_user_assignments 表
async function syncControllerUserAssignments(pool, validUserIds, isFullSync = false) {
  console.log('\n=== 同步 controller_user_assignments 表 ===');
  
  const isEmpty = await isTableEmpty('controller_user_assignments');
  const needFullSync = isFullSync || isEmpty;
  
  if (needFullSync) {
    console.log('  模式: 全量同步（表为空或强制全量）');
  } else {
    console.log('  模式: 增量同步');
  }
  
  const result = await pool.request().query(`
    SELECT 
      UserId, UnitId, type, AccessLevel,
      SendSMSAlarm, SendEmailAlarm, SendDataSummaryReport,
      SendWaterEnergyReport, SendTrendCharts,
      DateCreated
    FROM UserControllerAssignment
  `);
  
  console.log(`读取到 ${result.recordset.length} 条用户-控制器关系记录`);
  
  if (result.recordset.length === 0) {
    console.log('没有用户-控制器关系数据需要同步');
    return;
  }
  
  // 过滤掉无效的 UserId（在去重过程中被移除的）
  const recordsToSync = result.recordset.filter(record => validUserIds.has(record.UserId));
  
  if (recordsToSync.length < result.recordset.length) {
    const filteredCount = result.recordset.length - recordsToSync.length;
    console.log(`  过滤了 ${filteredCount} 条无效记录（对应的 UserId 在 users 表中不存在）`);
  }
  
  if (recordsToSync.length === 0) {
    console.log('没有有效的用户-控制器关系数据需要同步');
    return;
  }
  
  // 权限映射函数
  function mapPermissionLevel(type, accessLevel) {
    const t = type || 0;
    const level = accessLevel || 0;
    
    if (t === 1 && level === 3) return 'OWNER';
    if (t === 1 && level === 2) return 'NORMAL';
    if (t === 1 && level === 1) return 'READONLY';
    
    return 'NORMAL';
  }
  
  let inserted = 0;
  let updated = 0;
  let deleted = 0;
  
  if (needFullSync) {
    // 全量同步
    await prisma.$executeRawUnsafe('TRUNCATE TABLE controller_user_assignments CASCADE');
    console.log('✓ 已清空 controller_user_assignments 表');
  } else {
    // 增量同步：比较差异，只同步变化的记录
    console.log('  开始增量同步...');
    
    // 读取现有记录的所有字段用于比较
    const existingRecords = await prisma.$queryRawUnsafe(`
      SELECT user_id, unit_id, permission_level,
             send_sms_alarm, send_email_alarm, send_data_summary_report,
             send_water_energy_report, send_trend_charts,
             created_at, updated_at
      FROM controller_user_assignments
    `);
    
    const existingMap = new Map();
    existingRecords.forEach(r => {
      const key = `${r.user_id}|${r.unit_id}`;
      existingMap.set(key, r);
    });
    
    const sourceMap = new Map();
    recordsToSync.forEach(r => {
      const key = `${r.UserId}|${r.UnitId}`;
      sourceMap.set(key, r);
    });
    
    // 辅助函数：规范化值
    const normalizeValue = (val) => {
      if (val === null || val === undefined) return null;
      if (typeof val === 'boolean') return val;
      return String(val);
    };
    
    // 找出需要新增、更新和删除的记录
    const toInsert = [];
    const toUpdate = [];
    
    for (const record of recordsToSync) {
      const key = `${record.UserId}|${record.UnitId}`;
      const existing = existingMap.get(key);
      const permissionLevel = mapPermissionLevel(record.type, record.AccessLevel);
      
      if (!existing) {
        toInsert.push(record);
        logChange('INSERT', 'controller_user_assignments', record, 'user_id,unit_id', `${record.UserId},${record.UnitId}`);
      } else {
        // 比较所有字段
        const hasChanged = 
          normalizeValue(existing.permission_level) !== normalizeValue(permissionLevel) ||
          Boolean(existing.send_sms_alarm) !== Boolean(record.SendSMSAlarm === 1 || record.SendSMSAlarm === true || record.SendSMSAlarm === '1') ||
          Boolean(existing.send_email_alarm) !== Boolean(record.SendEmailAlarm === 1 || record.SendEmailAlarm === true || record.SendEmailAlarm === '1') ||
          Boolean(existing.send_data_summary_report) !== Boolean(record.SendDataSummaryReport === 1 || record.SendDataSummaryReport === true || record.SendDataSummaryReport === '1') ||
          Boolean(existing.send_water_energy_report) !== Boolean(record.SendWaterEnergyReport === 1 || record.SendWaterEnergyReport === true || record.SendWaterEnergyReport === '1') ||
          Boolean(existing.send_trend_charts) !== Boolean(record.SendTrendCharts === 1 || record.SendTrendCharts === true || record.SendTrendCharts === '1');
        
        if (hasChanged) {
          toUpdate.push(record);
          logChange('UPDATE', 'controller_user_assignments', record, 'user_id,unit_id', `${record.UserId},${record.UnitId}`);
        }
      }
    }
    
    // 找出需要删除的记录
    for (const [key] of existingMap.entries()) {
      if (!sourceMap.has(key)) {
        deleted++;
        const [userId, unitId] = key.split('|');
        logChange('DELETE', 'controller_user_assignments', null, 'user_id,unit_id', `${userId},${unitId}`);
      }
    }
    
    // 执行删除
    if (deleted > 0) {
      const toDeletePairs = Array.from(existingMap.keys()).filter(k => !sourceMap.has(k));
      const deleteConditions = toDeletePairs.map(key => {
        const [userId, unitId] = key.split('|');
        return `(user_id = ${escapeSQL(userId)} AND unit_id = ${escapeSQL(unitId)})`;
      });
      const deleteQuery = `DELETE FROM controller_user_assignments WHERE ${deleteConditions.join(' OR ')}`;
      await prisma.$executeRawUnsafe(deleteQuery);
    }
    
    // 执行新增
    if (toInsert.length > 0) {
      const batchSize = 500;
      for (let i = 0; i < toInsert.length; i += batchSize) {
        const batch = toInsert.slice(i, i + batchSize);
        const values = batch.map(record => {
          const permissionLevel = mapPermissionLevel(record.type, record.AccessLevel);
          return `(
            ${escapeSQL(record.UserId)},
            ${escapeSQL(record.UnitId)},
            '${permissionLevel}'::permission_level,
            ${toBoolean(record.SendSMSAlarm)},
            ${toBoolean(record.SendEmailAlarm)},
            ${toBoolean(record.SendDataSummaryReport)},
            ${toBoolean(record.SendWaterEnergyReport)},
            ${toBoolean(record.SendTrendCharts)},
            ${toTimestamp(record.DateCreated)},
            ${toTimestamp(record.DateCreated)}
          )`;
        }).join(', ');
        
        const query = `INSERT INTO controller_user_assignments (
          user_id, unit_id, permission_level,
          send_sms_alarm, send_email_alarm, send_data_summary_report,
          send_water_energy_report, send_trend_charts,
          created_at, updated_at
        ) VALUES ${values}`;
        
        await prisma.$executeRawUnsafe(query);
        inserted += batch.length;
      }
    }
    
    // 执行更新（只有真正有变化的记录）
    if (toUpdate.length > 0) {
      const batchSize = 500;
      for (let i = 0; i < toUpdate.length; i += batchSize) {
        const batch = toUpdate.slice(i, i + batchSize);
        const values = batch.map(record => {
          const permissionLevel = mapPermissionLevel(record.type, record.AccessLevel);
          return `(
            ${escapeSQL(record.UserId)},
            ${escapeSQL(record.UnitId)},
            '${permissionLevel}'::permission_level,
            ${toBoolean(record.SendSMSAlarm)},
            ${toBoolean(record.SendEmailAlarm)},
            ${toBoolean(record.SendDataSummaryReport)},
            ${toBoolean(record.SendWaterEnergyReport)},
            ${toBoolean(record.SendTrendCharts)},
            ${toTimestamp(record.DateCreated)},
            ${toTimestamp(record.DateCreated)}
          )`;
        }).join(', ');
        
        const query = `INSERT INTO controller_user_assignments (
          user_id, unit_id, permission_level,
          send_sms_alarm, send_email_alarm, send_data_summary_report,
          send_water_energy_report, send_trend_charts,
          created_at, updated_at
        ) VALUES ${values}
        ON CONFLICT (user_id, unit_id) DO UPDATE SET
          permission_level = EXCLUDED.permission_level,
          send_sms_alarm = EXCLUDED.send_sms_alarm,
          send_email_alarm = EXCLUDED.send_email_alarm,
          send_data_summary_report = EXCLUDED.send_data_summary_report,
          send_water_energy_report = EXCLUDED.send_water_energy_report,
          send_trend_charts = EXCLUDED.send_trend_charts,
          updated_at = EXCLUDED.updated_at`;
        
        await prisma.$executeRawUnsafe(query);
        updated += batch.length;
      }
    }
    
    console.log(`✓ controller_user_assignments 表增量同步完成: 新增 ${inserted} 条, 更新 ${updated} 条, 删除 ${deleted} 条`);
    return;
  }
  
  // 全量同步：批量插入
  const batchSize = 500;
  for (let i = 0; i < recordsToSync.length; i += batchSize) {
    const batch = recordsToSync.slice(i, i + batchSize);
    
    const values = batch.map(record => {
      const permissionLevel = mapPermissionLevel(record.type, record.AccessLevel);
      return `(
        ${escapeSQL(record.UserId)},
        ${escapeSQL(record.UnitId)},
        '${permissionLevel}'::permission_level,
        ${toBoolean(record.SendSMSAlarm)},
        ${toBoolean(record.SendEmailAlarm)},
        ${toBoolean(record.SendDataSummaryReport)},
        ${toBoolean(record.SendWaterEnergyReport)},
        ${toBoolean(record.SendTrendCharts)},
        ${toTimestamp(record.DateCreated)},
        ${toTimestamp(record.DateCreated)}
      )`;
    }).join(', ');
    
    const query = `
      INSERT INTO controller_user_assignments (
        user_id, unit_id, permission_level,
        send_sms_alarm, send_email_alarm, send_data_summary_report,
        send_water_energy_report, send_trend_charts,
        created_at, updated_at
      ) VALUES ${values}
      ON CONFLICT (user_id, unit_id) DO UPDATE SET
        permission_level = EXCLUDED.permission_level,
        send_sms_alarm = EXCLUDED.send_sms_alarm,
        send_email_alarm = EXCLUDED.send_email_alarm,
        send_data_summary_report = EXCLUDED.send_data_summary_report,
        send_water_energy_report = EXCLUDED.send_water_energy_report,
        send_trend_charts = EXCLUDED.send_trend_charts,
        updated_at = EXCLUDED.updated_at
    `;
    
    await prisma.$executeRawUnsafe(query);
    inserted += batch.length;
    console.log(`  已插入 ${inserted}/${recordsToSync.length} 条关系记录...`);
  }
  
  console.log(`✓ controller_user_assignments 表全量同步完成，共 ${inserted} 条记录`);
}

// 同步 controller_notes 表（从 SQL Server ControllerNotes 表）
async function syncControllerNotes(pool, validUserIds, isFullSync = false) {
  console.log('\n=== 同步 controller_notes 表 ===');
  
  const isEmpty = await isTableEmpty('controller_notes');
  const needFullSync = isFullSync || isEmpty;
  
  if (needFullSync) {
    console.log('  模式: 全量同步（表为空或强制全量）');
  } else {
    console.log('  模式: 增量同步');
  }
  
  try {
    // 从 SQL Server 查询 ControllerNotes 表
    const result = await pool.request().query(`
      SELECT 
        ID,
        UnitId,
        UserId,
        Notes,
        DateCreated
      FROM ControllerNotes
      ORDER BY ID
    `);
    
    console.log(`读取到 ${result.recordset.length} 条备注记录`);
    
    if (result.recordset.length === 0) {
      console.log('没有备注记录需要同步');
      return;
    }
    
    // 过滤掉无效的 UserId（如果 UserId 不在有效用户列表中）
    const recordsToSync = result.recordset.filter(record => {
      // 如果 UserId 为 null 或不在有效列表中，仍然保留记录，但 user_id 设为 null
      return record.UnitId && record.Notes;
    });
    
    if (recordsToSync.length < result.recordset.length) {
      const filteredCount = result.recordset.length - recordsToSync.length;
      console.log(`  过滤了 ${filteredCount} 条无效记录（UnitId 或 Notes 为空）`);
    }
    
    if (recordsToSync.length === 0) {
      console.log('没有有效的备注记录需要同步');
      return;
    }
    
    let inserted = 0;
    let updated = 0;
    let deleted = 0;
    
    if (needFullSync) {
      // 全量同步：清空表后插入所有记录
      await prisma.$executeRawUnsafe('TRUNCATE TABLE controller_notes CASCADE');
      console.log('✓ 已清空 controller_notes 表');
      
      const batchSize = 500;
      for (let i = 0; i < recordsToSync.length; i += batchSize) {
        const batch = recordsToSync.slice(i, i + batchSize);
        
        const values = batch.map(record => {
          // 提取日期（从 DateCreated）
          const dateCreated = record.DateCreated ? new Date(record.DateCreated) : null;
          const noteDate = dateCreated ? dateCreated.toISOString().split('T')[0] : null;
          
          // 处理 UserId：如果在有效列表中则使用，否则设为 NULL
          let userId = 'NULL';
          if (record.UserId && validUserIds.has(record.UserId)) {
            userId = escapeSQL(record.UserId);
          }
          
          return `(
            ${escapeSQL(record.ID)},
            ${escapeSQL(record.UnitId)},
            ${userId},
            ${escapeSQL(record.Notes)},
            ${noteDate ? `'${noteDate}'` : 'NULL'},
            'SQL_SERVER',
            ${escapeSQL(record.ID)},
            ${toTimestamp(record.DateCreated)},
            ${toTimestamp(record.DateCreated)}
          )`;
        }).join(', ');
        
        const query = `
          INSERT INTO controller_notes (
            note_id, unit_id, user_id, note_text, note_date,
            source_system, source_id, created_at, updated_at
          ) VALUES ${values}
        `;
        
        await prisma.$executeRawUnsafe(query);
        inserted += batch.length;
        console.log(`  已插入 ${inserted}/${recordsToSync.length} 条备注记录...`);
      }
      
      console.log(`✓ controller_notes 表全量同步完成，共 ${inserted} 条记录`);
    } else {
      // 增量同步：比较差异，只同步变化的记录
      console.log('  开始增量同步...');
      
      // 读取现有记录（使用 source_id 作为唯一标识）
      const existingRecords = await prisma.$queryRawUnsafe(`
        SELECT note_id, unit_id, user_id, note_text, note_date, source_id, created_at
        FROM controller_notes
      `);
      
      const existingMap = new Map();
      existingRecords.forEach(r => {
        existingMap.set(String(r.source_id), r);
      });
      
      const sourceMap = new Map();
      recordsToSync.forEach(r => {
        sourceMap.set(String(r.ID), r);
      });
      
      // 辅助函数：规范化值
      const normalizeValue = (val) => {
        if (val === null || val === undefined) return null;
        if (val instanceof Date) return val.toISOString();
        if (typeof val === 'boolean') return val;
        return String(val);
      };
      
      // 辅助函数：比较日期
      const compareDate = (val1, val2) => {
        if (!val1 && !val2) return true;
        if (!val1 || !val2) return false;
        const d1 = val1 instanceof Date ? val1 : new Date(val1);
        const d2 = val2 instanceof Date ? val2 : new Date(val2);
        return d1.getTime() === d2.getTime();
      };
      
      // 找出需要新增和更新的记录
      const toInsert = [];
      const toUpdate = [];
      
      for (const record of recordsToSync) {
        const sourceId = String(record.ID);
        const existing = existingMap.get(sourceId);
        
        if (!existing) {
          toInsert.push(record);
          logChange('INSERT', 'controller_notes', record, 'note_id', record.ID);
        } else {
          // 比较字段是否有变化
          const dateCreated = record.DateCreated ? new Date(record.DateCreated) : null;
          const noteDate = dateCreated ? dateCreated.toISOString().split('T')[0] : null;
          const existingNoteDate = existing.note_date ? new Date(existing.note_date).toISOString().split('T')[0] : null;
          
          let userId = null;
          if (record.UserId && validUserIds.has(record.UserId)) {
            userId = record.UserId;
          }
          
          const hasChanged = 
            normalizeValue(existing.unit_id) !== normalizeValue(record.UnitId) ||
            normalizeValue(existing.user_id) !== normalizeValue(userId) ||
            normalizeValue(existing.note_text) !== normalizeValue(record.Notes) ||
            normalizeValue(existingNoteDate) !== normalizeValue(noteDate) ||
            !compareDate(existing.created_at, record.DateCreated);
          
          if (hasChanged) {
            toUpdate.push(record);
            logChange('UPDATE', 'controller_notes', record, 'note_id', record.ID);
          }
        }
      }
      
      // 找出需要删除的记录（在目标中存在但源中不存在）
      for (const [sourceId, existing] of existingMap.entries()) {
        if (!sourceMap.has(sourceId)) {
          deleted++;
          logChange('DELETE', 'controller_notes', null, 'note_id', existing.note_id);
        }
      }
      
      // 执行新增
      if (toInsert.length > 0) {
        const batchSize = 500;
        for (let i = 0; i < toInsert.length; i += batchSize) {
          const batch = toInsert.slice(i, i + batchSize);
          const values = batch.map(record => {
            const dateCreated = record.DateCreated ? new Date(record.DateCreated) : null;
            const noteDate = dateCreated ? dateCreated.toISOString().split('T')[0] : null;
            
            let userId = 'NULL';
            if (record.UserId && validUserIds.has(record.UserId)) {
              userId = escapeSQL(record.UserId);
            }
            
            return `(
              ${escapeSQL(record.ID)},
              ${escapeSQL(record.UnitId)},
              ${userId},
              ${escapeSQL(record.Notes)},
              ${noteDate ? `'${noteDate}'` : 'NULL'},
              'SQL_SERVER',
              ${escapeSQL(record.ID)},
              ${toTimestamp(record.DateCreated)},
              ${toTimestamp(record.DateCreated)}
            )`;
          }).join(', ');
          
          const query = `INSERT INTO controller_notes (
            note_id, unit_id, user_id, note_text, note_date,
            source_system, source_id, created_at, updated_at
          ) VALUES ${values}`;
          
          await prisma.$executeRawUnsafe(query);
          inserted += batch.length;
        }
      }
      
      // 执行更新
      if (toUpdate.length > 0) {
        const batchSize = 500;
        for (let i = 0; i < toUpdate.length; i += batchSize) {
          const batch = toUpdate.slice(i, i + batchSize);
          const values = batch.map(record => {
            const dateCreated = record.DateCreated ? new Date(record.DateCreated) : null;
            const noteDate = dateCreated ? dateCreated.toISOString().split('T')[0] : null;
            
            let userId = 'NULL';
            if (record.UserId && validUserIds.has(record.UserId)) {
              userId = escapeSQL(record.UserId);
            }
            
            return `(
              ${escapeSQL(record.ID)},
              ${escapeSQL(record.UnitId)},
              ${userId},
              ${escapeSQL(record.Notes)},
              ${noteDate ? `'${noteDate}'` : 'NULL'},
              'SQL_SERVER',
              ${escapeSQL(record.ID)},
              ${toTimestamp(record.DateCreated)},
              ${toTimestamp(record.DateCreated)}
            )`;
          }).join(', ');
          
          const query = `INSERT INTO controller_notes (
            note_id, unit_id, user_id, note_text, note_date,
            source_system, source_id, created_at, updated_at
          ) VALUES ${values}
          ON CONFLICT (source_id) DO UPDATE SET
            unit_id = EXCLUDED.unit_id,
            user_id = EXCLUDED.user_id,
            note_text = EXCLUDED.note_text,
            note_date = EXCLUDED.note_date,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at`;
          
          await prisma.$executeRawUnsafe(query);
          updated += batch.length;
        }
      }
      
      // 执行删除
      if (deleted > 0) {
        const toDeleteSourceIds = Array.from(existingMap.keys()).filter(id => !sourceMap.has(id));
        const toDeleteNoteIds = toDeleteSourceIds.map(id => existingMap.get(id).note_id);
        const deleteQuery = `DELETE FROM controller_notes WHERE note_id IN (${toDeleteNoteIds.map(id => escapeSQL(id)).join(', ')})`;
        await prisma.$executeRawUnsafe(deleteQuery);
      }
      
      console.log(`✓ controller_notes 表增量同步完成: 新增 ${inserted} 条, 更新 ${updated} 条, 删除 ${deleted} 条`);
    }
  } catch (error) {
    console.error('✗ controller_notes 表同步失败:', error.message);
    console.error(error.stack);
    throw error;
  }
}

// 更新 billing_user_id（从权限关系表中获取）
async function updateBillingUserIds() {
  console.log('\n=== 更新 billing_user_id ===');
  
  try {
    // 从权限关系表中查找每个控制器有 OWNER 权限的用户
    const result = await prisma.$queryRaw`
      SELECT DISTINCT ON (cua.unit_id)
        cua.unit_id,
        cua.user_id
      FROM controller_user_assignments cua
      WHERE cua.permission_level = 'OWNER'
      ORDER BY cua.unit_id, cua.created_at
    `;
    
    if (result.length > 0) {
      console.log(`  找到 ${result.length} 个控制器需要更新 billing_user_id...`);
      let updated = 0;
      
      for (const record of result) {
        await prisma.$executeRawUnsafe(`
          UPDATE controllers 
          SET billing_user_id = ${record.user_id}
          WHERE unit_id = ${record.unit_id}
            AND (billing_user_id IS NULL OR billing_user_id != ${record.user_id})
        `);
        updated++;
      }
      
      console.log(`  ✓ 已更新 ${updated} 条记录的 billing_user_id`);
    } else {
      console.log('  没有找到需要更新的 billing_user_id');
    }
  } catch (error) {
    console.log('  ⚠ 更新 billing_user_id 失败:', error.message);
  }
}

// 初始化数据库结构（删除并重建所有表）
async function initializeDatabaseSchema() {
  console.log('\n' + '='.repeat(60));
  console.log('初始化数据库结构...');
  console.log('='.repeat(60));
  
  try {
    // 读取 schema SQL 文件
    const __filename = fileURLToPath(import.meta.url);
    const __dirname = dirname(__filename);
    const schemaPath = join(__dirname, '..', 'database-schema.sql');
    const schemaSQL = readFileSync(schemaPath, 'utf-8');
    
    console.log('  正在执行数据库结构重建...');
    
    // Prisma 的 $executeRawUnsafe 不支持多语句，需要逐条执行
    // 先尝试整体执行（可能会失败），然后逐条执行
    let useStatementByStatement = true;
    
    if (!useStatementByStatement) {
      try {
        await prisma.$executeRawUnsafe(schemaSQL);
        console.log('✓ 数据库结构重建完成（整体执行）\n');
        return;
      } catch (error) {
        console.log('  整体执行失败，改为逐条执行...');
      }
    }
    
    // 逐条执行 SQL 语句
    if (useStatementByStatement) {
      
      // 改进的 SQL 分割逻辑：正确处理函数定义和美元引号
      const statements = [];
      let current = '';
      let inDollarQuote = false;
      let dollarTag = '';
      let i = 0;
      
      while (i < schemaSQL.length) {
        const char = schemaSQL[i];
        const remaining = schemaSQL.substring(i);
        
        // 跳过注释行
        if (char === '-' && remaining.startsWith('--')) {
          const newlineIndex = remaining.indexOf('\n');
          if (newlineIndex !== -1) {
            i += newlineIndex + 1;
            continue;
          } else {
            break; // 文件结束
          }
        }
        
        // 检测美元引号开始（PostgreSQL 函数体使用）
        if (char === '$' && !inDollarQuote) {
          const match = remaining.match(/^\$([^$]*)\$/);
          if (match) {
            dollarTag = match[0];
            inDollarQuote = true;
            current += dollarTag;
            i += dollarTag.length;
            continue;
          }
        }
        
        // 检测美元引号结束
        if (inDollarQuote && remaining.startsWith(dollarTag)) {
          current += dollarTag;
          i += dollarTag.length;
          inDollarQuote = false;
          dollarTag = '';
          continue;
        }
        
        current += char;
        
        // 如果不在美元引号内，遇到分号就分割语句
        if (!inDollarQuote && char === ';') {
          const trimmed = current.trim();
          if (trimmed.length > 0) {
            statements.push(trimmed);
          }
          current = '';
        }
        
        i++;
      }
      
      // 添加最后一条语句（如果有）
      if (current.trim().length > 0) {
        statements.push(current.trim());
      }
      
      console.log(`  解析出 ${statements.length} 条 SQL 语句`);
      
      // 执行所有语句
      let successCount = 0;
      let errorCount = 0;
      const criticalErrors = [];
      const createTableStatements = [];
      
      // 辅助函数：检查表是否存在
      const tableExists = async (tableName) => {
        try {
          const result = await prisma.$queryRawUnsafe(`
            SELECT EXISTS (
              SELECT FROM information_schema.tables 
              WHERE table_schema = 'public' 
              AND table_name = '${tableName}'
            ) as exists
          `);
          return result[0]?.exists === true;
        } catch (error) {
          return false;
        }
      };
      
      // 辅助函数：删除表（如果存在）
      const dropTableIfExists = async (tableName) => {
        try {
          const exists = await tableExists(tableName);
          if (exists) {
            await prisma.$executeRawUnsafe(`DROP TABLE IF EXISTS ${tableName} CASCADE`);
            console.log(`  ✓ 已删除已存在的表: ${tableName}`);
            return true;
          }
          return false;
        } catch (error) {
          console.warn(`  ⚠ 删除表 ${tableName} 时出错: ${error.message}`);
          return false;
        }
      };
      
      for (let idx = 0; idx < statements.length; idx++) {
        const stmt = statements[idx];
        if (stmt.trim()) {
          // 如果是 CREATE TABLE 语句，先检查表是否存在
          if (stmt.toUpperCase().includes('CREATE TABLE')) {
            const tableMatch = stmt.match(/CREATE TABLE\s+(?:IF NOT EXISTS\s+)?["']?(\w+)["']?/i);
            if (tableMatch) {
              const tableName = tableMatch[1];
              // 先删除表（如果存在）
              await dropTableIfExists(tableName);
            }
          }
          
          try {
            await prisma.$executeRawUnsafe(stmt);
            successCount++;
            
            // 记录 CREATE TABLE 语句
            if (stmt.toUpperCase().includes('CREATE TABLE')) {
              const tableMatch = stmt.match(/CREATE TABLE\s+(?:IF NOT EXISTS\s+)?["']?(\w+)["']?/i);
              if (tableMatch) {
                createTableStatements.push(tableMatch[1]);
                // 调试：显示创建的表
                if (tableMatch[1].toLowerCase() === 'controller_notes') {
                  console.log(`  ✓ 成功创建表: ${tableMatch[1]}`);
                }
              }
            }
          } catch (err) {
            errorCount++;
            
            // 获取错误消息（可能在不同位置）
            const errorMessage = err.message || err.toString() || '';
            const errorCode = err.code || '';
            
            // 忽略"不存在"和"已存在"的错误（这些是正常的）
            const isIgnorableError = 
              errorMessage.includes('does not exist') || 
              errorMessage.includes('不存在') ||
              (errorMessage.includes('already exists') && !errorMessage.includes('constraint'));
            
            // 如果是约束已存在的错误（错误代码 42710 或消息包含 constraint already exists），说明表可能已存在，需要先删除
            const isConstraintExistsError = 
              errorCode === '42710' ||
              (errorMessage.includes('constraint') && errorMessage.includes('already exists'));
            
            if (isConstraintExistsError) {
              const tableMatch = stmt.match(/CREATE TABLE\s+(?:IF NOT EXISTS\s+)?["']?(\w+)["']?/i);
              if (tableMatch) {
                const tableName = tableMatch[1];
                console.warn(`  ⚠ 检测到表 ${tableName} 的约束已存在，尝试删除表后重新创建...`);
                try {
                  // 先删除表（CASCADE 会删除所有依赖）
                  await prisma.$executeRawUnsafe(`DROP TABLE IF EXISTS ${tableName} CASCADE`);
                  console.log(`  ✓ 已删除表 ${tableName}`);
                  
                  // 等待一下确保删除完成
                  await new Promise(resolve => setTimeout(resolve, 100));
                  
                  // 重新执行创建语句
                  await prisma.$executeRawUnsafe(stmt);
                  successCount++;
                  errorCount--; // 减少错误计数，因为已经成功处理
                  
                  // 记录创建的表
                  if (stmt.toUpperCase().includes('CREATE TABLE')) {
                    createTableStatements.push(tableName);
                  }
                  
                  console.log(`  ✓ 成功重新创建表: ${tableName}`);
                  continue; // 跳过后续的错误处理
                } catch (dropError) {
                  console.error(`  ✗ 处理表 ${tableName} 失败: ${dropError.message}`);
                  // 继续执行错误处理逻辑
                }
              }
            }
            
            // 对于 CREATE TABLE 语句，即使错误可忽略也要记录
            const isCreateTable = stmt.toUpperCase().includes('CREATE TABLE');
            
            if (!isIgnorableError || isCreateTable) {
              // 记录严重错误
              const errorMsg = err.message.substring(0, 200);
              const stmtPreview = stmt.substring(0, 100).replace(/\s+/g, ' ');
              
              if (isCreateTable) {
                const tableMatch = stmt.match(/CREATE TABLE\s+(?:IF NOT EXISTS\s+)?["']?(\w+)["']?/i);
                if (tableMatch) {
                  console.error(`  ✗ CREATE TABLE ${tableMatch[1]} 执行失败: ${errorMsg}`);
                  console.error(`     语句预览: ${stmtPreview}...`);
                } else {
                  console.error(`  ✗ CREATE TABLE 执行失败 (语句 ${idx + 1}): ${errorMsg}`);
                }
              } else {
                console.warn(`  ⚠ SQL 执行警告 (语句 ${idx + 1}): ${errorMsg}`);
                console.warn(`     语句预览: ${stmtPreview}...`);
              }
              
              criticalErrors.push({ index: idx + 1, error: errorMsg, statement: stmtPreview });
            }
          }
        }
      }
      
      console.log(`  执行完成: ${successCount} 条成功, ${errorCount} 条失败（部分失败是正常的）`);
      if (createTableStatements.length > 0) {
        console.log(`  创建的表: ${createTableStatements.join(', ')}`);
      }
      
      if (criticalErrors.length > 0) {
        console.warn(`  ⚠ 发现 ${criticalErrors.length} 个严重错误`);
        // 显示所有严重错误
        criticalErrors.forEach(err => {
          console.warn(`     错误 ${err.index}: ${err.error}`);
        });
        
        // 如果关键表创建失败，抛出错误
        const requiredTables = ['users', 'user_profiles', 'controllers', 'controller_user_assignments', 'controller_notes'];
        const createdTablesLower = createTableStatements.map(t => t.toLowerCase());
        const missingTables = requiredTables.filter(t => !createdTablesLower.includes(t));
        if (missingTables.length > 0) {
          console.error(`  ✗ 关键表创建失败: ${missingTables.join(', ')}`);
          throw new Error(`关键表创建失败: ${missingTables.join(', ')}`);
        }
      }
      
      console.log('✓ 数据库结构重建完成\n');
    }
  } catch (error) {
    console.error('✗ 数据库结构初始化失败:', error.message);
    throw error;
  }
}

// 主同步函数
// disconnectPrisma: 是否在同步完成后断开 Prisma 连接（默认 false，用于定时任务）
// isFullSync: 是否强制全量同步（默认 false，自动检测）
// rebuildSchema: 是否在同步前重建数据库结构（默认 true，首次同步时重建）
async function syncAllTables(disconnectPrisma = false, isFullSync = false, rebuildSchema = true) {
  let pool;
  try {
    console.log('='.repeat(60));
    console.log('开始同步所有表数据...');
    console.log('='.repeat(60));
    
    // 测试数据库连接
    pool = await sql.connect(sqlServerConfig);
    console.log('✓ 已连接到 SQL Server');
    
    // 测试 PostgreSQL 连接
    await prisma.$queryRaw`SELECT 1 AS test`;
    console.log('✓ 已连接到 PostgreSQL');
    
    // 第一步：重建数据库结构（删除并重建所有表）
    if (rebuildSchema) {
      await initializeDatabaseSchema();
      
      // 验证表是否创建成功
      console.log('  验证表结构...');
      try {
        await prisma.$queryRawUnsafe(`SELECT 1 FROM users LIMIT 1`);
        await prisma.$queryRawUnsafe(`SELECT 1 FROM user_profiles LIMIT 1`);
        await prisma.$queryRawUnsafe(`SELECT 1 FROM controllers LIMIT 1`);
        await prisma.$queryRawUnsafe(`SELECT 1 FROM controller_user_assignments LIMIT 1`);
        await prisma.$queryRawUnsafe(`SELECT 1 FROM controller_notes LIMIT 1`);
        console.log('✓ 所有表结构验证通过\n');
      } catch (error) {
        console.error('✗ 表结构验证失败:', error.message);
        throw new Error(`数据库表结构创建失败: ${error.message}`);
      }
      
      // 重建结构后，强制全量同步
      isFullSync = true;
    } else {
      // 检查是否需要全量同步（如果任何表为空，则全量同步）
      if (!isFullSync) {
        const usersEmpty = await isTableEmpty('users');
        if (usersEmpty) {
          console.log('检测到 users 表为空，将执行全量同步');
          isFullSync = true;
        }
      }
    }
    
    // 按顺序同步表（注意外键依赖关系）
    // 1. 先同步 users（被其他表引用）
    const { validUserIds } = await syncUsers(pool, isFullSync);
    
    // 2. 同步 user_profiles（依赖 users，只同步有效的 UserId）
    await syncUserProfiles(pool, validUserIds, isFullSync);
    
    // 3. 同步 controllers（依赖 users 的 billing_user_id，只使用有效的 UserId）
    await syncControllers(pool, validUserIds, isFullSync);
    
    // 4. 最后同步 controller_user_assignments（依赖 users 和 controllers，只同步有效的 UserId）
    await syncControllerUserAssignments(pool, validUserIds, isFullSync);
    
    // 5. 同步 controller_notes（从 SQL Server ControllerNotes 表）
    await syncControllerNotes(pool, validUserIds, isFullSync);
    
    // 6. 更新 billing_user_id（从权限关系表中获取 OWNER 权限的用户）
    await updateBillingUserIds();
    
    console.log('\n' + '='.repeat(60));
    console.log('✓ 所有表数据同步完成！');
    console.log('='.repeat(60));
    
  } catch (error) {
    console.error('\n✗ 同步失败:', error.message);
    console.error(error.stack);
    throw error; // 重新抛出错误，让调用者知道同步失败
  } finally {
    if (pool) {
      await pool.close();
    }
    // 只有在明确要求时才断开 Prisma 连接（例如直接运行此脚本时）
    if (disconnectPrisma) {
      await prisma.$disconnect();
    }
  }
}

// 导出同步函数供其他模块使用
export { syncAllTables };

// 如果直接运行此文件，则执行同步
// 检查是否作为主模块运行
const __filename = fileURLToPath(import.meta.url);
const isMainModule = process.argv[1] && resolve(process.argv[1]) === resolve(__filename);

if (isMainModule) {
  // 直接运行时，同步完成后断开连接
  syncAllTables(true).catch(() => {
    process.exit(1);
  });
}
