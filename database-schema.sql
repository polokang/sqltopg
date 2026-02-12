-- =====================================================
-- PostgreSQL 数据库表结构设计
-- 基于经典的多对多关系设计模式
-- =====================================================

-- =====================================================
-- 清理阶段：删除所有已存在的对象
-- =====================================================

-- 删除视图（依赖表）
DROP VIEW IF EXISTS user_controllers CASCADE;
DROP VIEW IF EXISTS controller_user_permissions CASCADE;
DROP VIEW IF EXISTS user_full_info CASCADE;

-- 删除表（按依赖关系顺序，先删除依赖表）
DROP TABLE IF EXISTS controller_notes CASCADE;
DROP TABLE IF EXISTS controller_user_assignments CASCADE;
DROP TABLE IF EXISTS controllers CASCADE;
DROP TABLE IF EXISTS user_profiles CASCADE;
DROP TABLE IF EXISTS users CASCADE;

-- 删除类型
DROP TYPE IF EXISTS permission_level CASCADE;

-- 删除函数
DROP FUNCTION IF EXISTS update_updated_at_column() CASCADE;

-- =====================================================
-- 创建阶段：重新建立所有对象
-- =====================================================

-- =====================================================
-- 1. 用户核心信息表
-- =====================================================
CREATE TABLE users (
    user_id INTEGER PRIMARY KEY,
    company_id INTEGER NOT NULL,
    user_type_id INTEGER NOT NULL,
    mobile VARCHAR(50) DEFAULT '0000000000',
    email VARCHAR(100) NOT NULL,
    name VARCHAR(100) NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    activated BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- 索引
    CONSTRAINT users_email_unique UNIQUE (email)
);

CREATE INDEX idx_users_company_id ON users(company_id);
CREATE INDEX idx_users_user_type_id ON users(user_type_id);
CREATE INDEX idx_users_activated ON users(activated);

COMMENT ON TABLE users IS '用户核心信息表，存储用户的基本身份信息';
COMMENT ON COLUMN users.user_id IS '用户唯一标识（主键）';
COMMENT ON COLUMN users.company_id IS '所属公司ID';
COMMENT ON COLUMN users.user_type_id IS '用户类型ID';
COMMENT ON COLUMN users.mobile IS '手机号码（选填，默认值：0000000000）';
COMMENT ON COLUMN users.email IS '邮箱地址（唯一约束）';
COMMENT ON COLUMN users.activated IS '是否激活';

-- =====================================================
-- 2. 用户扩展信息表（登录、会话、安全等）
-- =====================================================
CREATE TABLE user_profiles (
    user_id INTEGER PRIMARY KEY REFERENCES users(user_id) ON DELETE CASCADE,
    last_login_time TIMESTAMP WITH TIME ZONE,
    last_logout_time TIMESTAMP WITH TIME ZONE,
    is_read BOOLEAN DEFAULT false,
    is_phone_verified BOOLEAN DEFAULT false,
    requires_two_factor_auth BOOLEAN DEFAULT false,
    logo VARCHAR(255),
    production BOOLEAN DEFAULT false,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

COMMENT ON TABLE user_profiles IS '用户扩展信息表，存储登录、会话、安全等辅助信息';
COMMENT ON COLUMN user_profiles.last_login_time IS '最后登录时间';
COMMENT ON COLUMN user_profiles.last_logout_time IS '最后登出时间';
COMMENT ON COLUMN user_profiles.is_phone_verified IS '手机是否已验证';
COMMENT ON COLUMN user_profiles.requires_two_factor_auth IS '是否需要双因素认证';

-- =====================================================
-- 3. 控制器（设备）信息表
-- =====================================================
CREATE TABLE controllers (
    unit_id INTEGER PRIMARY KEY,
    sim_card_no VARCHAR(50) NOT NULL,
    billing_user_id INTEGER REFERENCES users(user_id) ON DELETE SET NULL,
    system_id VARCHAR(100) NOT NULL,
    firmware_version VARCHAR(50),
    activated BOOLEAN NOT NULL DEFAULT false,
    model_type VARCHAR(50),
    date_created TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    date_last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- 其他配置信息（可选，根据业务需求保留）
    creator VARCHAR(255),
    tags TEXT[], -- 标签数组（从 Suburb 字段转换，用逗号分割）
    timezone_id VARCHAR(50)
    -- 索引（无唯一约束，允许用户自定义设备名称）
);

CREATE INDEX idx_controllers_billing_user_id ON controllers(billing_user_id);
CREATE INDEX idx_controllers_activated ON controllers(activated);
CREATE INDEX idx_controllers_date_created ON controllers(date_created);

COMMENT ON TABLE controllers IS '控制器（设备）信息表，存储控制器的核心信息和配置';
COMMENT ON COLUMN controllers.unit_id IS '控制器唯一标识';
COMMENT ON COLUMN controllers.sim_card_no IS 'SIM卡号';
COMMENT ON COLUMN controllers.billing_user_id IS '计费用户ID（账单负责人）';
COMMENT ON COLUMN controllers.system_id IS '系统ID（设备名称，用户可自定义，允许重复）';
COMMENT ON COLUMN controllers.firmware_version IS '固件版本';
COMMENT ON COLUMN controllers.activated IS '是否激活';
COMMENT ON COLUMN controllers.tags IS '标签数组（从旧数据库 Suburb 字段转换，原为逗号分割的字符串）';

-- =====================================================
-- 4. 用户-控制器权限关系表（多对多关系）
-- =====================================================

-- 权限级别枚举类型
CREATE TYPE permission_level AS ENUM ('OWNER', 'NORMAL', 'READONLY');

CREATE TABLE controller_user_assignments (
    assignment_id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    unit_id INTEGER NOT NULL REFERENCES controllers(unit_id) ON DELETE CASCADE,
    permission_level permission_level NOT NULL DEFAULT 'NORMAL',
    
    -- 通知设置（可选，根据业务需求）
    send_sms_alarm BOOLEAN DEFAULT false,
    send_email_alarm BOOLEAN DEFAULT false,
    send_data_summary_report BOOLEAN DEFAULT false,
    send_water_energy_report BOOLEAN DEFAULT false,
    send_trend_charts BOOLEAN DEFAULT false,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- 唯一约束：同一用户对同一控制器只能有一条记录
    CONSTRAINT controller_user_assignments_unique UNIQUE (user_id, unit_id)
);

CREATE INDEX idx_controller_user_assignments_user_id ON controller_user_assignments(user_id);
CREATE INDEX idx_controller_user_assignments_unit_id ON controller_user_assignments(unit_id);
CREATE INDEX idx_controller_user_assignments_permission_level ON controller_user_assignments(permission_level);

COMMENT ON TABLE controller_user_assignments IS '用户-控制器权限关系表，记录用户对控制器的访问权限';
COMMENT ON COLUMN controller_user_assignments.unit_id IS '控制器ID';
COMMENT ON COLUMN controller_user_assignments.permission_level IS '权限级别：OWNER(拥有者), NORMAL(普通用户), READONLY(只读用户)';
COMMENT ON COLUMN controller_user_assignments.send_sms_alarm IS '是否发送短信告警';
COMMENT ON COLUMN controller_user_assignments.send_email_alarm IS '是否发送邮件告警';

-- =====================================================
-- 5. 触发器：自动更新 updated_at 字段
-- =====================================================

-- 为所有表创建更新触发器函数
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_users_updated_at
    BEFORE UPDATE ON users
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_user_profiles_updated_at
    BEFORE UPDATE ON user_profiles
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- controllers 表使用 date_last_updated 字段，需要专门的触发器函数
CREATE OR REPLACE FUNCTION update_controllers_date_last_updated()
RETURNS TRIGGER AS $$
BEGIN
    NEW.date_last_updated = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_controllers_updated_at
    BEFORE UPDATE ON controllers
    FOR EACH ROW
    EXECUTE FUNCTION update_controllers_date_last_updated();

CREATE TRIGGER update_controller_user_assignments_updated_at
    BEFORE UPDATE ON controller_user_assignments
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- =====================================================
-- 6. 视图：便于查询的常用视图
-- =====================================================

-- 用户完整信息视图
CREATE VIEW user_full_info AS
SELECT 
    u.user_id,
    u.company_id,
    u.user_type_id,
    u.mobile,
    u.email,
    u.name,
    u.activated,
    up.last_login_time,
    up.last_logout_time,
    up.is_phone_verified,
    up.requires_two_factor_auth,
    u.created_at,
    u.updated_at
FROM users u
LEFT JOIN user_profiles up ON u.user_id = up.user_id;

-- 控制器用户权限视图
CREATE VIEW controller_user_permissions AS
SELECT 
    cua.assignment_id,
    cua.user_id,
    u.name AS user_name,
    u.email AS user_email,
    cua.unit_id,
    c.system_id,
    c.sim_card_no,
    cua.permission_level,
    cua.send_sms_alarm,
    cua.send_email_alarm,
    cua.created_at,
    cua.updated_at
FROM controller_user_assignments cua
JOIN users u ON cua.user_id = u.user_id
JOIN controllers c ON cua.unit_id = c.unit_id;

-- 用户控制器列表视图
CREATE VIEW user_controllers AS
SELECT 
    u.user_id,
    u.name AS user_name,
    u.email AS user_email,
    c.unit_id,
    c.system_id,
    c.sim_card_no,
    c.activated AS controller_activated,
    cua.permission_level,
    cua.created_at AS assigned_at
FROM users u
JOIN controller_user_assignments cua ON u.user_id = cua.user_id
JOIN controllers c ON cua.unit_id = c.unit_id
WHERE u.activated = true AND c.activated = true;

-- =====================================================
-- 7. 控制器备注记录表（同步自 SQL Server ControllerNotes）
-- =====================================================

CREATE TABLE controller_notes (
    -- 主键（对应 SQL Server 的 ID）
    note_id INTEGER PRIMARY KEY,
    
    -- 外键：关联到控制器
    unit_id INTEGER NOT NULL REFERENCES controllers(unit_id) ON DELETE CASCADE,
    
    -- 外键：关联到用户（可选）
    user_id INTEGER REFERENCES users(user_id) ON DELETE SET NULL,
    
    -- 核心字段
    note_text TEXT NOT NULL,                    -- 备注内容（对应 SQL Server 的 Notes）
    note_date DATE,                              -- 备注日期（从 DateCreated 提取）
    
    -- 元数据字段
    source_system VARCHAR(50) DEFAULT 'SQL_SERVER', -- 数据来源系统
    source_id INTEGER,                           -- 源系统记录ID（SQL Server 的 ID，用于去重）
    
    -- 时间戳
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(), -- 记录创建时间（对应 SQL Server 的 DateCreated）
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() -- 记录更新时间
    
);

CREATE INDEX idx_controller_notes_unit_id ON controller_notes(unit_id);
CREATE INDEX idx_controller_notes_user_id ON controller_notes(user_id);
CREATE INDEX idx_controller_notes_note_date ON controller_notes(note_date);
CREATE INDEX idx_controller_notes_created_at ON controller_notes(created_at DESC);
CREATE INDEX idx_controller_notes_unit_date ON controller_notes(unit_id, note_date DESC NULLS LAST);
CREATE INDEX idx_controller_notes_source_id ON controller_notes(source_id);

COMMENT ON TABLE controller_notes IS '设备备注记录表，同步自 SQL Server ControllerNotes 表';
COMMENT ON COLUMN controller_notes.note_id IS '记录唯一标识（对应 SQL Server ControllerNotes.ID）';
COMMENT ON COLUMN controller_notes.unit_id IS '控制器ID（外键关联 controllers.unit_id）';
COMMENT ON COLUMN controller_notes.user_id IS '用户ID（外键关联 users.user_id，对应 SQL Server ControllerNotes.UserId）';
COMMENT ON COLUMN controller_notes.note_text IS '备注内容（对应 SQL Server ControllerNotes.Notes）';
COMMENT ON COLUMN controller_notes.note_date IS '备注日期（从 DateCreated 提取日期部分）';
COMMENT ON COLUMN controller_notes.source_system IS '数据来源系统（SQL_SERVER）';
COMMENT ON COLUMN controller_notes.source_id IS '源系统记录ID（SQL Server ControllerNotes.ID，用于去重）';

-- 触发器：自动更新 updated_at 字段
CREATE OR REPLACE FUNCTION update_controller_notes_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_controller_notes_updated_at
    BEFORE UPDATE ON controller_notes
    FOR EACH ROW
    EXECUTE FUNCTION update_controller_notes_updated_at();

-- =====================================================
-- 8. 示例查询
-- =====================================================

-- 查询用户的所有控制器及其权限
-- SELECT * FROM user_controllers WHERE user_id = 1;

-- 查询控制器的所有用户及其权限
-- SELECT * FROM controller_user_permissions WHERE unit_id = 1;

-- 查询用户完整信息
-- SELECT * FROM user_full_info WHERE user_id = 1;
