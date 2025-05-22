-- ========================================
-- 1. Lookup Tables
-- ========================================

-- Lookup table for standard procedure codes (e.g., CPT, HCPCS)
CREATE TABLE dbo.Procedure_Codes (
    Procedure_Code NVARCHAR(10) NOT NULL PRIMARY KEY,  -- E.g., "99213"
    Description NVARCHAR(255) NOT NULL                 -- Description of the procedure
);

-- Lookup table for diagnosis codes (e.g., ICD-10)
CREATE TABLE dbo.Diagnosis_Codes (
    Diagnosis_Code NVARCHAR(10) NOT NULL PRIMARY KEY,  -- E.g., "E11.9"
    Description NVARCHAR(255) NOT NULL                 -- Description of the diagnosis
);

-- Lookup table for claim statuses
CREATE TABLE dbo.Claim_Status_Codes (
    Claim_Status_Code NVARCHAR(20) NOT NULL PRIMARY KEY,  -- E.g., 'Submitted', 'Denied', 'Approved'
    Description NVARCHAR(100) NOT NULL                    -- Human-readable explanation of status
);

-- ========================================
-- 2. Core Tables
-- ========================================

-- Patient master table
CREATE TABLE dbo.Patients (
    Patient_ID UNIQUEIDENTIFIER NOT NULL PRIMARY KEY DEFAULT NEWID(),  -- Unique identifier for the patient
    First_Name NVARCHAR(100) NOT NULL,
    Last_Name NVARCHAR(100) NOT NULL,
    Date_of_Birth DATE NOT NULL,
    Gender CHAR(1) NOT NULL CHECK (Gender IN ('M', 'F', 'O')),  -- 'M' = Male, 'F' = Female, 'O' = Other
    Insurance_ID NVARCHAR(50) NULL,  -- External insurance identifier for the patient
    Created_At DATETIME2 NOT NULL DEFAULT SYSDATETIME()
);

-- Provider table (e.g., doctor or clinic)
CREATE TABLE dbo.Providers (
    Provider_ID UNIQUEIDENTIFIER NOT NULL PRIMARY KEY DEFAULT NEWID(),  -- Unique ID for provider
    Provider_Name NVARCHAR(255) NOT NULL,  -- Full name of clinic or individual provider
    NPI NVARCHAR(20) NOT NULL UNIQUE,      -- National Provider Identifier
    Specialty NVARCHAR(100) NULL,
    Tax_ID NVARCHAR(50) NULL,
    Created_At DATETIME2 NOT NULL DEFAULT SYSDATETIME()
);

-- Payer table (insurance companies)
CREATE TABLE dbo.Payers (
    Payer_ID UNIQUEIDENTIFIER NOT NULL PRIMARY KEY DEFAULT NEWID(),  -- Unique ID for payer
    Payer_Name NVARCHAR(255) NOT NULL,  -- Name of insurance provider
    Payer_Code NVARCHAR(50) NOT NULL UNIQUE,  -- Internal or standard payer code
    Created_At DATETIME2 NOT NULL DEFAULT SYSDATETIME()
);

-- Main claim table
CREATE TABLE dbo.Claims (
    Claim_ID UNIQUEIDENTIFIER NOT NULL PRIMARY KEY DEFAULT NEWID(),  -- Unique identifier for claim
    Patient_ID UNIQUEIDENTIFIER NOT NULL,  -- FK to Patients
    Provider_ID UNIQUEIDENTIFIER NOT NULL, -- FK to Providers
    Payer_ID UNIQUEIDENTIFIER NOT NULL,    -- FK to Payers
    Claim_Date DATETIME2 NOT NULL,         -- Date when the claim was filed
    Claim_Status_Code NVARCHAR(20) NOT NULL, -- FK to Claim_Status_Codes
    Total_Claim_Amount MONEY NOT NULL CHECK (Total_Claim_Amount >= 0),
    Diagnosis_Code NVARCHAR(10) NOT NULL,   -- FK to Diagnosis_Codes
    Created_At DATETIME2 NOT NULL DEFAULT SYSDATETIME(),
    CONSTRAINT FK_Claims_Patients FOREIGN KEY (Patient_ID) REFERENCES dbo.Patients(Patient_ID),
    CONSTRAINT FK_Claims_Providers FOREIGN KEY (Provider_ID) REFERENCES dbo.Providers(Provider_ID),
    CONSTRAINT FK_Claims_Payers FOREIGN KEY (Payer_ID) REFERENCES dbo.Payers(Payer_ID),
    CONSTRAINT FK_Claims_Status FOREIGN KEY (Claim_Status_Code) REFERENCES dbo.Claim_Status_Codes(Claim_Status_Code),
    CONSTRAINT FK_Claims_Diagnosis FOREIGN KEY (Diagnosis_Code) REFERENCES dbo.Diagnosis_Codes(Diagnosis_Code)
);

-- Claim line items table (multiple lines per claim)
CREATE TABLE dbo.Claim_Lines (
    Claim_Line_ID UNIQUEIDENTIFIER NOT NULL PRIMARY KEY DEFAULT NEWID(),  -- Unique line item ID
    Claim_ID UNIQUEIDENTIFIER NOT NULL,       -- FK to Claims
    Line_Number INT NOT NULL,                 -- Line number within the claim
    Procedure_Code NVARCHAR(10) NOT NULL,     -- FK to Procedure_Codes
    Service_Date DATE NOT NULL,               -- Date of service for the procedure
    Charge_Amount MONEY NOT NULL CHECK (Charge_Amount >= 0),
    Units INT NOT NULL CHECK (Units >= 1),
    CONSTRAINT FK_ClaimLines_Claims FOREIGN KEY (Claim_ID) REFERENCES dbo.Claims(Claim_ID),
    CONSTRAINT FK_ClaimLines_Procedure FOREIGN KEY (Procedure_Code) REFERENCES dbo.Procedure_Codes(Procedure_Code)
);

-- Payment table to store actual payments against claims
CREATE TABLE dbo.Payments (
    Payment_ID UNIQUEIDENTIFIER NOT NULL PRIMARY KEY DEFAULT NEWID(),  -- Unique payment ID
    Claim_ID UNIQUEIDENTIFIER NOT NULL,      -- FK to Claims
    Paid_Amount MONEY NOT NULL CHECK (Paid_Amount >= 0),
    Payment_Date DATETIME2 NOT NULL,
    Payment_Method NVARCHAR(50) NOT NULL,  -- E.g., 'EFT', 'Check'
    Created_At DATETIME2 NOT NULL DEFAULT SYSDATETIME(),
    CONSTRAINT FK_Payments_Claims FOREIGN KEY (Claim_ID) REFERENCES dbo.Claims(Claim_ID)
);

-- Member address table (decoupled to allow multiple addresses per patient if needed)
CREATE TABLE dbo.Patient_Addresses (
    Address_ID UNIQUEIDENTIFIER NOT NULL PRIMARY KEY DEFAULT NEWID(),
    Patient_ID UNIQUEIDENTIFIER NOT NULL,  -- FK to Patients
    Address_Type NVARCHAR(50) NOT NULL CHECK (Address_Type IN ('Home', 'Mailing', 'Work')),  -- Type of address
    Address_Line1 NVARCHAR(255) NOT NULL,
    Address_Line2 NVARCHAR(255) NULL,
    City NVARCHAR(100) NOT NULL,
    State NVARCHAR(50) NOT NULL,
    Zip_Code NVARCHAR(20) NOT NULL,
    Country NVARCHAR(100) NOT NULL DEFAULT 'USA',
    Created_At DATETIME2 NOT NULL DEFAULT SYSDATETIME(),
    CONSTRAINT FK_Addresses_Patient FOREIGN KEY (Patient_ID) REFERENCES dbo.Patients(Patient_ID)
);

-- ========================================
-- 3. Indexes and Performance Considerations
-- ========================================

-- Index on Claims.Claim_Date for date range queries
CREATE NONCLUSTERED INDEX IX_Claims_ClaimDate ON dbo.Claims(Claim_Date);

-- Index on Claim_Lines.Procedure_Code for filtering by procedure
CREATE NONCLUSTERED INDEX IX_ClaimLines_ProcedureCode ON dbo.Claim_Lines(Procedure_Code);

-- Index on Claims.Claim_Status_Code for reporting by status
CREATE NONCLUSTERED INDEX IX_Claims_Status ON dbo.Claims(Claim_Status_Code);

-- Index on Payments.Payment_Date for analytics
CREATE NONCLUSTERED INDEX IX_Payments_PaymentDate ON dbo.Payments(Payment_Date);

-- Index on Patients.Last_Name for search
CREATE NONCLUSTERED INDEX IX_Patients_LastName ON dbo.Patients(Last_Name);

-- ========================================
-- 4. Optional Constraints
-- ========================================

-- Enforce uniqueness for line number per claim
ALTER TABLE dbo.Claim_Lines
ADD CONSTRAINT UQ_Claim_Line_Unique UNIQUE (Claim_ID, Line_Number);

-- ========================================
-- End of Schema
-- ========================================
