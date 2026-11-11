--
-- PostgreSQL database dump
--

\restrict kMCS2cdR2gRRunLdfVYtdGUjnerHZ7SEp65CouPhZfpEs3tShmkQzu7reF7CMhu

-- Dumped from database version 16.15 (Debian 16.15-1.pgdg13+2)
-- Dumped by pg_dump version 16.15 (Debian 16.15-1.pgdg13+2)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: ingest_flagged_fn(); Type: FUNCTION; Schema: public; Owner: fraud
--

CREATE FUNCTION public.ingest_flagged_fn() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    INSERT INTO flagged_transactions
    SELECT NEW.*
    ON CONFLICT (transaction_id) DO NOTHING;

    INSERT INTO alerts (transaction_id)
    VALUES (NEW.transaction_id)
    ON CONFLICT (transaction_id) DO NOTHING;

    RETURN NEW;
END;
$$;


ALTER FUNCTION public.ingest_flagged_fn() OWNER TO fraud;

--
-- Name: ingest_stats_fn(); Type: FUNCTION; Schema: public; Owner: fraud
--

CREATE FUNCTION public.ingest_stats_fn() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    INSERT INTO fraud_stats_by_minute (minute, txns, flagged_txns, total_amount, max_risk_score)
    VALUES (NEW.minute, NEW.txns, NEW.flagged_txns, NEW.total_amount, NEW.max_risk_score)
    ON CONFLICT (minute) DO UPDATE SET
        txns           = EXCLUDED.txns,
        flagged_txns   = EXCLUDED.flagged_txns,
        total_amount   = EXCLUDED.total_amount,
        max_risk_score = EXCLUDED.max_risk_score,
        updated_at     = now();
    RETURN NEW;
END;
$$;


ALTER FUNCTION public.ingest_stats_fn() OWNER TO fraud;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: alerts; Type: TABLE; Schema: public; Owner: fraud
--

CREATE TABLE public.alerts (
    alert_id bigint NOT NULL,
    transaction_id text NOT NULL,
    llm_status text DEFAULT 'PENDING'::text NOT NULL,
    severity text,
    explanation text,
    suggested_action text,
    llm_attempts integer DEFAULT 0 NOT NULL,
    last_error text,
    case_status text DEFAULT 'OPEN'::text NOT NULL,
    assigned_to text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT alerts_case_status_check CHECK ((case_status = ANY (ARRAY['OPEN'::text, 'IN_REVIEW'::text, 'CONFIRMED_FRAUD'::text, 'FALSE_POSITIVE'::text]))),
    CONSTRAINT alerts_llm_status_check CHECK ((llm_status = ANY (ARRAY['PENDING'::text, 'DONE'::text, 'FAILED'::text])))
);


ALTER TABLE public.alerts OWNER TO fraud;

--
-- Name: alerts_alert_id_seq; Type: SEQUENCE; Schema: public; Owner: fraud
--

CREATE SEQUENCE public.alerts_alert_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.alerts_alert_id_seq OWNER TO fraud;

--
-- Name: alerts_alert_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: fraud
--

ALTER SEQUENCE public.alerts_alert_id_seq OWNED BY public.alerts.alert_id;


--
-- Name: flagged_transactions; Type: TABLE; Schema: public; Owner: fraud
--

CREATE TABLE public.flagged_transactions (
    transaction_id text NOT NULL,
    card_id text NOT NULL,
    amount numeric(14,2) NOT NULL,
    currency text,
    merchant text,
    merchant_category text,
    city text,
    latitude double precision,
    longitude double precision,
    device_id text,
    event_time timestamp with time zone NOT NULL,
    txn_count_5m integer,
    txn_count_10s integer,
    distance_km double precision,
    speed_kmh double precision,
    avg_amount_prior double precision,
    amount_ratio double precision,
    is_new_device boolean,
    triggered_rules text NOT NULL,
    risk_score integer NOT NULL,
    scored_at timestamp with time zone,
    label_is_fraud boolean,
    label_fraud_type text
);


ALTER TABLE public.flagged_transactions OWNER TO fraud;

--
-- Name: fraud_daily_summary; Type: TABLE; Schema: public; Owner: fraud
--

CREATE TABLE public.fraud_daily_summary (
    day date NOT NULL,
    txns bigint,
    flagged_txns bigint,
    flag_rate double precision,
    total_amount double precision,
    critical_alerts integer,
    high_alerts integer,
    medium_alerts integer,
    low_alerts integer,
    open_cases integer,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.fraud_daily_summary OWNER TO fraud;

--
-- Name: fraud_stats_by_minute; Type: TABLE; Schema: public; Owner: fraud
--

CREATE TABLE public.fraud_stats_by_minute (
    minute timestamp with time zone NOT NULL,
    txns bigint NOT NULL,
    flagged_txns bigint NOT NULL,
    total_amount double precision,
    max_risk_score integer,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.fraud_stats_by_minute OWNER TO fraud;

--
-- Name: ingest_flagged; Type: VIEW; Schema: public; Owner: fraud
--

CREATE VIEW public.ingest_flagged AS
 SELECT transaction_id,
    card_id,
    amount,
    currency,
    merchant,
    merchant_category,
    city,
    latitude,
    longitude,
    device_id,
    event_time,
    txn_count_5m,
    txn_count_10s,
    distance_km,
    speed_kmh,
    avg_amount_prior,
    amount_ratio,
    is_new_device,
    triggered_rules,
    risk_score,
    scored_at,
    label_is_fraud,
    label_fraud_type
   FROM public.flagged_transactions;


ALTER VIEW public.ingest_flagged OWNER TO fraud;

--
-- Name: ingest_stats; Type: VIEW; Schema: public; Owner: fraud
--

CREATE VIEW public.ingest_stats AS
 SELECT minute,
    txns,
    flagged_txns,
    total_amount,
    max_risk_score,
    updated_at
   FROM public.fraud_stats_by_minute;


ALTER VIEW public.ingest_stats OWNER TO fraud;

--
-- Name: alerts alert_id; Type: DEFAULT; Schema: public; Owner: fraud
--

ALTER TABLE ONLY public.alerts ALTER COLUMN alert_id SET DEFAULT nextval('public.alerts_alert_id_seq'::regclass);


--
-- Data for Name: alerts; Type: TABLE DATA; Schema: public; Owner: fraud
--

COPY public.alerts (alert_id, transaction_id, llm_status, severity, explanation, suggested_action, llm_attempts, last_error, case_status, assigned_to, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: flagged_transactions; Type: TABLE DATA; Schema: public; Owner: fraud
--

COPY public.flagged_transactions (transaction_id, card_id, amount, currency, merchant, merchant_category, city, latitude, longitude, device_id, event_time, txn_count_5m, txn_count_10s, distance_km, speed_kmh, avg_amount_prior, amount_ratio, is_new_device, triggered_rules, risk_score, scored_at, label_is_fraud, label_fraud_type) FROM stdin;
\.


--
-- Data for Name: fraud_daily_summary; Type: TABLE DATA; Schema: public; Owner: fraud
--

COPY public.fraud_daily_summary (day, txns, flagged_txns, flag_rate, total_amount, critical_alerts, high_alerts, medium_alerts, low_alerts, open_cases, updated_at) FROM stdin;
2026-11-01	6092	603	0.0989822718319107	14393511.67	7	154	442	0	603	2026-11-02 09:24:20.85583+00
\.


--
-- Data for Name: fraud_stats_by_minute; Type: TABLE DATA; Schema: public; Owner: fraud
--

COPY public.fraud_stats_by_minute (minute, txns, flagged_txns, total_amount, max_risk_score, updated_at) FROM stdin;
\.


--
-- Name: alerts_alert_id_seq; Type: SEQUENCE SET; Schema: public; Owner: fraud
--

SELECT pg_catalog.setval('public.alerts_alert_id_seq', 1, false);


--
-- Name: alerts alerts_pkey; Type: CONSTRAINT; Schema: public; Owner: fraud
--

ALTER TABLE ONLY public.alerts
    ADD CONSTRAINT alerts_pkey PRIMARY KEY (alert_id);


--
-- Name: alerts alerts_transaction_id_key; Type: CONSTRAINT; Schema: public; Owner: fraud
--

ALTER TABLE ONLY public.alerts
    ADD CONSTRAINT alerts_transaction_id_key UNIQUE (transaction_id);


--
-- Name: flagged_transactions flagged_transactions_pkey; Type: CONSTRAINT; Schema: public; Owner: fraud
--

ALTER TABLE ONLY public.flagged_transactions
    ADD CONSTRAINT flagged_transactions_pkey PRIMARY KEY (transaction_id);


--
-- Name: fraud_daily_summary fraud_daily_summary_pkey; Type: CONSTRAINT; Schema: public; Owner: fraud
--

ALTER TABLE ONLY public.fraud_daily_summary
    ADD CONSTRAINT fraud_daily_summary_pkey PRIMARY KEY (day);


--
-- Name: fraud_stats_by_minute fraud_stats_by_minute_pkey; Type: CONSTRAINT; Schema: public; Owner: fraud
--

ALTER TABLE ONLY public.fraud_stats_by_minute
    ADD CONSTRAINT fraud_stats_by_minute_pkey PRIMARY KEY (minute);


--
-- Name: idx_alerts_case; Type: INDEX; Schema: public; Owner: fraud
--

CREATE INDEX idx_alerts_case ON public.alerts USING btree (case_status);


--
-- Name: idx_alerts_pending; Type: INDEX; Schema: public; Owner: fraud
--

CREATE INDEX idx_alerts_pending ON public.alerts USING btree (alert_id) WHERE (llm_status = 'PENDING'::text);


--
-- Name: idx_flagged_card; Type: INDEX; Schema: public; Owner: fraud
--

CREATE INDEX idx_flagged_card ON public.flagged_transactions USING btree (card_id, event_time DESC);


--
-- Name: idx_flagged_time; Type: INDEX; Schema: public; Owner: fraud
--

CREATE INDEX idx_flagged_time ON public.flagged_transactions USING btree (event_time DESC);


--
-- Name: ingest_flagged ingest_flagged_trg; Type: TRIGGER; Schema: public; Owner: fraud
--

CREATE TRIGGER ingest_flagged_trg INSTEAD OF INSERT ON public.ingest_flagged FOR EACH ROW EXECUTE FUNCTION public.ingest_flagged_fn();


--
-- Name: ingest_stats ingest_stats_trg; Type: TRIGGER; Schema: public; Owner: fraud
--

CREATE TRIGGER ingest_stats_trg INSTEAD OF INSERT ON public.ingest_stats FOR EACH ROW EXECUTE FUNCTION public.ingest_stats_fn();


--
-- Name: alerts alerts_transaction_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: fraud
--

ALTER TABLE ONLY public.alerts
    ADD CONSTRAINT alerts_transaction_id_fkey FOREIGN KEY (transaction_id) REFERENCES public.flagged_transactions(transaction_id);


--
-- Name: SCHEMA public; Type: ACL; Schema: -; Owner: pg_database_owner
--

GRANT USAGE ON SCHEMA public TO grafana_ro;
GRANT USAGE ON SCHEMA public TO analyst_app;


--
-- Name: TABLE alerts; Type: ACL; Schema: public; Owner: fraud
--

GRANT SELECT ON TABLE public.alerts TO grafana_ro;
GRANT SELECT ON TABLE public.alerts TO analyst_app;


--
-- Name: COLUMN alerts.case_status; Type: ACL; Schema: public; Owner: fraud
--

GRANT UPDATE(case_status) ON TABLE public.alerts TO analyst_app;


--
-- Name: COLUMN alerts.assigned_to; Type: ACL; Schema: public; Owner: fraud
--

GRANT UPDATE(assigned_to) ON TABLE public.alerts TO analyst_app;


--
-- Name: COLUMN alerts.updated_at; Type: ACL; Schema: public; Owner: fraud
--

GRANT UPDATE(updated_at) ON TABLE public.alerts TO analyst_app;


--
-- Name: TABLE flagged_transactions; Type: ACL; Schema: public; Owner: fraud
--

GRANT SELECT ON TABLE public.flagged_transactions TO grafana_ro;


--
-- Name: COLUMN flagged_transactions.transaction_id; Type: ACL; Schema: public; Owner: fraud
--

GRANT SELECT(transaction_id) ON TABLE public.flagged_transactions TO analyst_app;


--
-- Name: COLUMN flagged_transactions.card_id; Type: ACL; Schema: public; Owner: fraud
--

GRANT SELECT(card_id) ON TABLE public.flagged_transactions TO analyst_app;


--
-- Name: COLUMN flagged_transactions.amount; Type: ACL; Schema: public; Owner: fraud
--

GRANT SELECT(amount) ON TABLE public.flagged_transactions TO analyst_app;


--
-- Name: COLUMN flagged_transactions.currency; Type: ACL; Schema: public; Owner: fraud
--

GRANT SELECT(currency) ON TABLE public.flagged_transactions TO analyst_app;


--
-- Name: COLUMN flagged_transactions.merchant; Type: ACL; Schema: public; Owner: fraud
--

GRANT SELECT(merchant) ON TABLE public.flagged_transactions TO analyst_app;


--
-- Name: COLUMN flagged_transactions.merchant_category; Type: ACL; Schema: public; Owner: fraud
--

GRANT SELECT(merchant_category) ON TABLE public.flagged_transactions TO analyst_app;


--
-- Name: COLUMN flagged_transactions.city; Type: ACL; Schema: public; Owner: fraud
--

GRANT SELECT(city) ON TABLE public.flagged_transactions TO analyst_app;


--
-- Name: COLUMN flagged_transactions.latitude; Type: ACL; Schema: public; Owner: fraud
--

GRANT SELECT(latitude) ON TABLE public.flagged_transactions TO analyst_app;


--
-- Name: COLUMN flagged_transactions.longitude; Type: ACL; Schema: public; Owner: fraud
--

GRANT SELECT(longitude) ON TABLE public.flagged_transactions TO analyst_app;


--
-- Name: COLUMN flagged_transactions.device_id; Type: ACL; Schema: public; Owner: fraud
--

GRANT SELECT(device_id) ON TABLE public.flagged_transactions TO analyst_app;


--
-- Name: COLUMN flagged_transactions.event_time; Type: ACL; Schema: public; Owner: fraud
--

GRANT SELECT(event_time) ON TABLE public.flagged_transactions TO analyst_app;


--
-- Name: COLUMN flagged_transactions.txn_count_5m; Type: ACL; Schema: public; Owner: fraud
--

GRANT SELECT(txn_count_5m) ON TABLE public.flagged_transactions TO analyst_app;


--
-- Name: COLUMN flagged_transactions.txn_count_10s; Type: ACL; Schema: public; Owner: fraud
--

GRANT SELECT(txn_count_10s) ON TABLE public.flagged_transactions TO analyst_app;


--
-- Name: COLUMN flagged_transactions.distance_km; Type: ACL; Schema: public; Owner: fraud
--

GRANT SELECT(distance_km) ON TABLE public.flagged_transactions TO analyst_app;


--
-- Name: COLUMN flagged_transactions.speed_kmh; Type: ACL; Schema: public; Owner: fraud
--

GRANT SELECT(speed_kmh) ON TABLE public.flagged_transactions TO analyst_app;


--
-- Name: COLUMN flagged_transactions.avg_amount_prior; Type: ACL; Schema: public; Owner: fraud
--

GRANT SELECT(avg_amount_prior) ON TABLE public.flagged_transactions TO analyst_app;


--
-- Name: COLUMN flagged_transactions.amount_ratio; Type: ACL; Schema: public; Owner: fraud
--

GRANT SELECT(amount_ratio) ON TABLE public.flagged_transactions TO analyst_app;


--
-- Name: COLUMN flagged_transactions.is_new_device; Type: ACL; Schema: public; Owner: fraud
--

GRANT SELECT(is_new_device) ON TABLE public.flagged_transactions TO analyst_app;


--
-- Name: COLUMN flagged_transactions.triggered_rules; Type: ACL; Schema: public; Owner: fraud
--

GRANT SELECT(triggered_rules) ON TABLE public.flagged_transactions TO analyst_app;


--
-- Name: COLUMN flagged_transactions.risk_score; Type: ACL; Schema: public; Owner: fraud
--

GRANT SELECT(risk_score) ON TABLE public.flagged_transactions TO analyst_app;


--
-- Name: COLUMN flagged_transactions.scored_at; Type: ACL; Schema: public; Owner: fraud
--

GRANT SELECT(scored_at) ON TABLE public.flagged_transactions TO analyst_app;


--
-- Name: TABLE fraud_stats_by_minute; Type: ACL; Schema: public; Owner: fraud
--

GRANT SELECT ON TABLE public.fraud_stats_by_minute TO grafana_ro;
GRANT SELECT ON TABLE public.fraud_stats_by_minute TO analyst_app;


--
-- PostgreSQL database dump complete
--

\unrestrict kMCS2cdR2gRRunLdfVYtdGUjnerHZ7SEp65CouPhZfpEs3tShmkQzu7reF7CMhu

