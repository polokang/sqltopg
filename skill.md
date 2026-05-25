- 需要确定的几点：
	1. ControllerInfo 表里有OwnerId 和 CompanyId 字段. CompanyId代表目前管理该设备的公司。 Owner可以改变CompanyId和BillingEmail。



分析一下整个 sqltopg项目，该项目的目的是将 SQL Server 数据库中的 Aquareporter 项目所需要用到的表中的数据经过一定的筛选和过滤插入到新的 PostgreSQL 数据库中。分析完后，新建一个新的 migration-base.js 脚本执行数据迁移。原则如下：
1. SQL Server 数据库中的数据只读取，不能修改或删除
2. PostgreSQL 数据库中的表及视图，再新建表和视图，确保数据的纯净，目前涉及到的表(CompanyInfo, ControllerInfo, UserControllerAssignment, UserInfo, UserProfiles); 目前涉及到的视图（ControllerUserPermissions, UserControllers, UserFullInfo）. 主要是我不确定删除表之前，是否需要先删除相关的视图和约束，如果不需要，则可以不用理会这三个视图。然后再帮我新建这五张表 CompanyInfo, ControllerInfo, UserControllerAssignment, UserInfo, UserProfiles 
3. 数据不是单纯拷贝，而是有一定的逻辑：


将 SQL Server 数据库中 [AquariusEmailDB].[dbo].[UserInfomation] 表的数据拷贝到 PostgreSQL 数据库中的 "public"."UserInfo"表和UserProfiles 表中

1. 后期需要去掉UserProfiles表中的Production和



AquariusEmailDB.UserInfomation.Mobile -> CompanyInfo.phone


docker run -d --name sqltopg -p 3000:3000 adminpage.azurecr.io/sqltopg:v1.0



SELECT * FROM [AquariusEmailDB].[dbo].[ControllerInfo] where AccManagerId in (4906, 5870, 6284, 7592, 8294, 9059, 9235, 9837, 9897, 9901, 10806, 10807, 10929)
select * from UserInfomation where UserId in (4906, 5870, 6284, 7592, 8294, 9059, 9235, 9837, 9897, 9901, 10806, 10807, 10929)
select * from CompanyInfomation where CompanyId in (2007,2073,3007,3042,8888,3001,3066,2052,3021,3065)


-- 同步 Users
现在帮我写数据库迁移-UserInfo的脚本migration-user.js,将[AquariusEmailDB].[dbo].[UserInfomation] 表的数据拷贝到 PostgreSQL 数据库中的 "public"."UserInfo"表，规则如下：
s.userinfo.UserId -> p.Userinfo.UserId
s.userinfo.CompanyId -> p.Userinfo.CompanyId
s.userinfo.Email -> p.Userinfo.Email
如果该用户的s.userinfo.Activated=1， 则EmailVerified=true，否则EmailVerified=false
s.userinfo.Mobile -> p.Userinfo.Mobile
s.userinfo.IsPhoneVerified -> p.Userinfo.MobileVerified
s.userinfo.UserName -> p.Userinfo.Name
s.userinfo.PasswordHash -> p.Userinfo.PasswordHash
s.userinfo.Logo -> p.Userinfo.Avatar
如果s.userinfo.UserTypeId=22 则说明该条用户记录是 'super' 用户，Roles 数组中要有['super']
如果s.userinfo.Production=1 则说明该条用户记录是 'manager' 用户，Roles 数组中要有['manager'],且ManagedCompanyIds数组中，要包含该用户的CompanyId
其他情况Roles都是['operator']
Roles 字段备注：super | manager | operator | owner 
s.userinfo.Activated -> p.Userinfo.Activated
s.userinfo.RequiresTwoFactorAuth -> p.Userinfo.MfaEnabled
s.userinfo.LastLoginTime -> p.Userinfo.LastLoginTime
s.userinfo.DateCreated -> p.Userinfo.CreatedAt
s.userinfo.LastUpdateDate -> p.Userinfo.UpdatedAt


-- 同步Company
migration-company.js 脚本中再增加一些手动插入的语句：
insert into CompanyInfo (3066,'Rare-Enviro',9092,'RAR001','Rare-Enviro','0428220648','Richard@rare-enviro.com.au','YourLogoGoesToHere.JPG',true,'2023-11-13 15:51:48.500','2023-11-13 15:51:48.500')

-- 同步Controllers
现在请帮我做数据迁移ControllerInfo的脚本 migration-controller.js. 将[AquariusEmailDB].[dbo].[ControllerInfo] as sqController 拷贝到 PostgreSQL 数据库中的 "public"."ControllerInfo" as pgController
sqController.[UnitId] -> pgController.UnitId
sqController.[SerialNo] -> pgController.SerialNo
sqController.[SIMCardNo] -> pgController.SIMCardNo
sqController.[AccManagerId] -> pgController.OwnerId
  pgController.CompanyId 需要根据 sqController.[AccManagerId] 去 [AquariusEmailDB].[dbo].[UserInfomation]表里找[UserId]=sqController.[AccManagerId] 那条记录的[CompanyId]
sqController.[SystemID] -> pgController.SystemId
sqController.[FirmwareVersion] -> pgController.FirmwareVersion
sqController.[ControllerModel] -> pgController.ModelType
sqController.[LinuxTimeZoneId] -> pgController.TimezoneId
sqController.[ProductionStatus] -> pgController.Status
sqController.[Activated] -> pgController.Activated
sqController.[DateCreated] -> pgController.DateCreated
sqController.[DateLastUpdate] -> pgController.DateLastUpdated
sqController.[SiteLocation] -> pgController.[Creator]
pgController.Tags 需要解析 sqController.[Suburb]字段，如果该字段不为NULL，则根据字符","分割成字符串数组。
pgController.BillingEmail字段需要根据sqController.[AccManagerId] 去 [AquariusEmailDB].[dbo].[UserInfomation]表里找[UserId]=sqController.[AccManagerId] 那条记录的[Email]
sqController.[Barcode] -> pgController.BarCode
sqController.[CustPO] -> pgController.PoNumber
sqController.[Serials] -> pgController.SpareParts
sqController.[Notes] -> pgController.Notes

-- 同步UserControllerAssignment，PermissionLevel 说明（0=READONLY / 50=NORMAL / 100=OWNER）
再帮我写一个同步 UserControllerAssignment 表的脚本migration-assignment.js, 将[AquariusEmailDB].[dbo].[UserControllerAssignment] 同步到PostgreSQL 数据库 UserControllerAssignment 中
[SeqKey] -> Id
[UserId] -> UserId
[UnitId] -> UnitId
[AccessLevel]如果是0则Permission等于0，如果[AccessLevel]=1则Permission等于50
[SendSMSAlarm] -> SendSmsAlarm
[SendEmailAlarm] -> SendEmailAlarm


请给我整理并规划一下Aquareporter人员管理，权限管理，设备管理的需求，目前涉及到的表都在 PostgreSQL 数据库中的UserInfo,ControllerInfo,CompanyInfo,UserControllerAssignment
先帮我分析一下这几张表的结构，看看能否满足目前公司的业务需求：
1. 用户分为：super,manager,owner,operator等角色,一个用户可以有多个角色，也可以管理多个公司
2. 角色是manager的用户，可以管理ManagedCompanyIds所对应的公司所属的员工和设备
3. 每个设备都有一个owner，owner可以更改设备的管理公司(CompanyId);只有该设备的owner才可以更改该设备的 BillingEmail
4. manager可以通过 User Management 页面修改属于他可以管理的公司下的员工资料，并分配和取消分配公司员工的属于该公司的所有设备
	- 点击某个员工后，进入该员工的详情页面
	- 员工列表
	- 员工详情页上半部分是该员工的基本资料，下半部分列出该员工可以管理的设备列表
5. 通过 controller 列表页进入controller详情页后，可以看到该设备的Status,Chart,Data,Event等tab，并可以通过About Tab 进入可以访问该设备的用户列表
	- Controller 列表页增加一列Assign 按钮，manager可以通过该按钮将设备取消或者分配该manager公司下的员工。如果登录用户是Manager或者有NORMAL权限的Operator
	- 如果是owner 则该设备再增加一个Manage 按钮，通过该按钮，owner 可以给该设备更换管理公司和BillingEmail(选择管理公司的流程我还没有想好。如果列出数据库里的公司，是不是会泄露商业)
6. 不是所有用户都需要有CompanyId. 