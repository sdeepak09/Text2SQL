SET ANSI_NULLS ON
_GO
SET QUOTED_IDENTIFIER ON
_GO

CREATE TABLE [dbo].[Patients]( -- Stores core information about each patient/member.
    [MEMBER] [varchar](50) NOT NULL,            -- Unique member identifier.
    [FIRST_NAME] [varchar](100) NOT NULL,       -- Patient's first name.
    [LAST_NAME] [varchar](100) NOT NULL,        -- Patient's last name.
    [DOB] [smalldatetime] NOT NULL,             -- Patient's date of birth.
    [GNDR] [varchar](50) NOT NULL,              -- Gender of the patient (e.g., active, discharged).
    [ADDRESS_1] [varchar](255) NULL,            -- Primary address line.
    [CITY] [varchar](100) NULL,                 -- City of residence.
    [STATE] [varchar](50) NULL,                 -- State of residence.
    [ZIP_CODE] [varchar](20) NULL               -- Postal/ZIP code.
    CONSTRAINT [PK_Patients] PRIMARY KEY CLUSTERED
    (
        [MEMBER] ASC
    )
) ON [PRIMARY]
_GO

CREATE TABLE [dbo].[ADMISSIONS]( -- Captures detailed records of patient hospital admissions.
    [ADMISSION_ID] [int] IDENTITY(1,1) NOT NULL,-- Unique identifier for each admission record.
    [MEMBER] [varchar](50) NOT NULL,            -- Unique member identifier, links to Patients.
    [CONFINEMENT_NBR] [tinyint] NOT NULL,       -- Confinement number for tracking individual admissions.
    [EPISODE_NBA] [smallint] NOT NULL,          -- Episode number identifying a specific episode of care.
    [ETG] [int] NOT NULL,                       -- The 9-digit ETG number assigned to the confinement.
    [ADMIT_DT] [varchar](50) NOT NULL,          -- Earliest From_DT of the Room & Board revenue code service records.
    [DISCHARGE_DT] [varchar](50) NOT NULL,      -- Most recent TO_DT of the Room & Board revenue code service records.
    [LENGTH_OF_STAY] [tinyint] NOT NULL,        -- Type of episode based on relevant categorization or procedure done.
    [EPISODE_TYPE] [tinyint] NOT NULL,          -- Type of episode based on relevant categorization or procedure done.
    [CONFINEMENT_TYPE] [tinyint] NOT NULL,      -- Type of confinement indicating the nature of the admission.
    [PROV_ID] [datetime2](7) NOT NULL,          -- Provider ID of the facility from the Room & Board revenue code records.
    [RESPONSIBLE_PROV_ID] [varchar](1) NULL,    -- Responsible provider ID for the admission.
    [PATIENT_STS] [nvarchar](1) NULL,           -- Status of the patient during confinement (e.g., active, discharged).
    [GNDR] [nvarchar](50) NOT NULL,             -- Gender of the patient.
    [ADMSN_AGE] [tinyint] NOT NULL,             -- Admission age of the patient at the time of confinement.
    [DISCHARGE_AGE] [tinyint] NOT NULL,         -- Discharge age of the patient at the time of admission.
    [FACILITY_TYPE] [tinyint] NOT NULL,         -- Type of facility where the admission occurred.
    [BED_TYPE] [tinyint] NOT NULL,              -- Type of bed assigned to the patient during confinement.
    [PROC_CD] [nvarchar](1) NULL,               -- Procedure code for any specific procedures performed during confinement.
    [ICD_DIAG_CD] [nvarchar](50) NOT NULL,      -- Primary diagnosis code from the most recent Room & Board service record.
    [ICD_PROC_CD] [nvarchar](1) NULL,           -- Procedure code for any specific procedures performed.
    [ICD_TYPE] [tinyint] NOT NULL,              -- Type of ICD code indicating version (e.g., ICD-10).
    [TOTAL_ALLOWED] [float] NOT NULL,           -- Total allowed amounts from records within the confinement.
    [TOTAL_PAID] [float] NOT NULL,              -- Total paid amounts from records within the confinement.
    [FACILITY_ALLOWED] [float] NOT NULL,        -- Facility allowed amounts from records within the confinement.
    [FACILITY_PAID] [float] NOT NULL,           -- Facility paid amounts from records within the confinement.
    [ROOM_BOARD_ALLOWED] [float] NOT NULL,      -- Allowed amounts for room and board during confinement.
    [ROOM_BOARD_PAID] [float] NOT NULL,         -- Paid amounts for room and board during confinement.
    [ANCILLARY_INPATIENT_ALLOWED] [float] NOT NULL, -- Allowed ancillary costs for inpatient services.
    [ANCILLARY_INPATIENT_PAID] [float] NOT NULL, -- Paid ancillary costs for inpatient services.
    [CLINICIAN_ALLOWED] [float] NOT NULL,       -- Allowed amounts for services provided by clinicians.
    [CLINICIAN_PAID] [float] NOT NULL,          -- Paid amounts for services provided by clinicians.
    [OTH_ALLOWED] [float] NOT NULL,             -- Other allowed amounts during confinement.
    [OTH_PAID] [float] NOT NULL,                -- Other paid amounts during confinement.
    [EPISODE_ALLOWED] [float] NOT NULL,         -- Total allowed for the entire episode of care.
    [EPISODE_PAID] [float] NOT NULL,            -- Total paid for the entire episode of care.
    [NON_EPISODE_ALLOWED] [float] NOT NULL,     -- Allowed amounts outside of the episode context.
    [NON_EPISODE_PAID] [float] NOT NULL,        -- Paid amounts outside of the episode context.
    [SUSPECT_CONFINEMENT] [tinyint] NOT NULL,   -- Flag indicating if the confinement is suspect.
    [CONFINEMENT_CLASSIFICATION] [tinyint] NOT NULL, -- Classification of the confinement for reporting.
    [RESERVED] [nvarchar](1) NULL               -- Reserved for future use or additional information.
    CONSTRAINT [PK_Admissions] PRIMARY KEY CLUSTERED
    (
        [ADMISSION_ID] ASC
    )
) ON [PRIMARY]
_GO

-- Add Foreign Key constraint to link Admissions to Patients
ALTER TABLE [dbo].[ADMISSIONS] WITH CHECK ADD CONSTRAINT [FK_Admissions_Patients] FOREIGN KEY([MEMBER])
REFERENCES [dbo].[Patients] ([MEMBER])
_GO
ALTER TABLE [dbo].[ADMISSIONS] CHECK CONSTRAINT [FK_Admissions_Patients]
_GO

CREATE TABLE [dbo].[CLINMARK_T]( -- Tracks clinical indicators for members, recording occurrences.
    [MEMBER] [varchar](32) NOT NULL,            -- Unique member identifier.
    [RULEID] [int] NOT NULL,                    -- Unique identifier of the clinical indicator.
    [IA_TIME] [tinyint] NOT NULL,               -- Numeric identifier of the IA_TIME period. The earliest period is IA_TIME = 1.
    [MIN_DT] [smalldatetime] NOT NULL,          -- Date of first occurrence for this member and clinical indicator during the time period.
    [MAX_DT] [smalldatetime] NOT NULL,          -- Date of last occurrence for this member and clinical indicator during the time period.
    [OCCURRENCES] [smallint] NOT NULL           -- Number of services / unique days that were observed.
    CONSTRAINT [PK_ClinMarkT] PRIMARY KEY CLUSTERED
    (
        [MEMBER] ASC,
        [RULEID] ASC,
        [IA_TIME] ASC
    )
) ON [PRIMARY]
_GO

-- Add Foreign Key constraint to link ClinMark_T to Patients
ALTER TABLE [dbo].[CLINMARK_T] WITH CHECK ADD CONSTRAINT [FK_ClinMarkT_Patients] FOREIGN KEY([MEMBER])
REFERENCES [dbo].[Patients] ([MEMBER])
_GO
ALTER TABLE [dbo].[CLINMARK_T] CHECK CONSTRAINT [FK_ClinMarkT_Patients]
_GO

CREATE TABLE [dbo].[CASD]( -- Seems to hold case or classification-related data.
    [CASE_ID] [int] NOT NULL,                   -- Unique identifier for the case.
    [SHORTDESC] [varchar](120) NOT NULL,        -- Short description.
    [LONGDESC] [varchar](1000) NULL,            -- Long description.
    [CAT_ID] [int] NOT NULL,                    -- Category identifier.
    [CAT_DESC] [varchar](120) NOT NULL          -- Category description.
    CONSTRAINT [PK_CASD] PRIMARY KEY CLUSTERED
    (
        [CASE_ID] ASC
    )
) ON [PRIMARY]
_GO