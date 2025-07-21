CREATE SCHEMA IF NOT EXISTS crm;
CREATE SCHEMA IF NOT EXISTS public;


-- -- Or just add the constraints you need
CREATE TABLE public.profiles AS SELECT * FROM clone_public.profiles WHERE id = 'ac035322-c347-4352-96d8-0d8f96280baa';
ALTER TABLE public.profiles ADD PRIMARY KEY (id);


CREATE TABLE crm.companies AS SELECT * FROM clone_crm.companies WHERE business_id = 'ac035322-c347-4352-96d8-0d8f96280baa';
ALTER TABLE crm.companies ADD PRIMARY KEY (id);

-- Create table with desired structure
CREATE TABLE crm.contacts (
    id uuid PRIMARY KEY NOT NULL DEFAULT gen_random_uuid(),
    business_id varchar(255) NOT NULL REFERENCES public.profiles(id),
    email varchar(255),
    company_id uuid REFERENCES crm.companies(id),
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    updated_at timestamp without time zone NOT NULL DEFAULT now(),
    custom_variables JSONB
);

-- Insert data
INSERT INTO crm.contacts (id, business_id, email, company_id, created_at, updated_at, custom_variables)
SELECT 
    c.id, 
    c.business_id, 
    c.email, 
    c.company_id, 
    c.created_at, 
    c.updated_at,
    COALESCE(
        jsonb_object_agg(cv.variable_schema_id, cv.value) FILTER (WHERE cv.variable_schema_id IS NOT NULL),
        '{}'::jsonb
    ) as custom_variables
FROM clone_crm.contacts c
LEFT JOIN clone_crm.contact_variables cv ON c.id = cv.contact_id 
WHERE c.business_id = 'ac035322-c347-4352-96d8-0d8f96280baa'
GROUP BY c.id, c.business_id, c.email, c.company_id, c.created_at, c.updated_at;

-- A general-purpose GIN
CREATE INDEX contacts_idx_custom_gin
    ON crm.contacts
    USING gin (custom_variables jsonb_path_ops);