--
-- PostgreSQL database dump
--

\restrict tELJXfacr5xbgtuLmFGtrX2ExQ5ZhkfnPSMrreMoKmEgRfJF2ij99cjQS10g3cK

-- Dumped from database version 18.4
-- Dumped by pg_dump version 18.4

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: product_analytics; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.product_analytics (
    product character varying(50) NOT NULL,
    transaction_count bigint DEFAULT 0 NOT NULL,
    total_quantity bigint DEFAULT 0 NOT NULL,
    last_updated timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.product_analytics OWNER TO postgres;

--
-- Data for Name: product_analytics; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.product_analytics (product, transaction_count, total_quantity, last_updated) FROM stdin;
Toffee	121	6242	2026-10-10 14:05:45.383506
Chocolate	109	5422	2026-10-10 14:05:45.383506
Gum	95	4791	2026-10-10 14:05:45.383506
Candy	106	4924	2026-10-10 14:05:45.383506
Biscuit	111	5818	2026-10-10 14:05:45.383506
\.


--
-- Name: product_analytics product_analytics_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.product_analytics
    ADD CONSTRAINT product_analytics_pkey PRIMARY KEY (product);


--
-- PostgreSQL database dump complete
--

\unrestrict tELJXfacr5xbgtuLmFGtrX2ExQ5ZhkfnPSMrreMoKmEgRfJF2ij99cjQS10g3cK

