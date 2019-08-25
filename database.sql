DROP TABLE public.weo;

CREATE TABLE public.weo
(
    country character varying COLLATE pg_catalog."default",
    subject_descriptor character varying COLLATE pg_catalog."default",
    units character varying COLLATE pg_catalog."default",
    scale character varying COLLATE pg_catalog."default",
    y2017 character varying COLLATE pg_catalog."default",
    y2018 character varying COLLATE pg_catalog."default",
    y2019 character varying COLLATE pg_catalog."default",
    y2020 character varying COLLATE pg_catalog."default",
    y2021 character varying COLLATE pg_catalog."default",
    y2022 character varying COLLATE pg_catalog."default"
)

select * from weo

--DROP TABLE public.bli;

CREATE TABLE public.bli
(
    country character varying COLLATE pg_catalog."default",
    dwellings_without_basic_facilities character varying COLLATE pg_catalog."default",
    housing_expenditure character varying COLLATE pg_catalog."default",
    rooms_per_person character varying COLLATE pg_catalog."default",
    household_net_income character varying COLLATE pg_catalog."default",
    household_net_wealth character varying COLLATE pg_catalog."default",
    labour_market_insecurity character varying COLLATE pg_catalog."default",
    employment_rate character varying COLLATE pg_catalog."default",
    longterm_unemployment_rate character varying COLLATE pg_catalog."default",
    personal_earnings character varying COLLATE pg_catalog."default",
    quality_support_network character varying COLLATE pg_catalog."default",
    educational_attainment character varying COLLATE pg_catalog."default",
    student_skills character varying COLLATE pg_catalog."default",
    years_in_education character varying COLLATE pg_catalog."default",
    air_pollution character varying COLLATE pg_catalog."default",
    water_quality character varying COLLATE pg_catalog."default",
    Stakeholder_engagement_for_developing_regulations character varying COLLATE pg_catalog."default",
    voter_turnout character varying COLLATE pg_catalog."default",
    selfreported_health character varying COLLATE pg_catalog."default",
    life_satisfaction character varying COLLATE pg_catalog."default",
    feeling_safe_walking_alone_at_night character varying COLLATE pg_catalog."default",
    homicide_rate character varying COLLATE pg_catalog."default",
    employees_working_very_long_hours character varying COLLATE pg_catalog."default",
    time_devoted_to_leisur_and_personal_care character varying COLLATE pg_catalog."default"
)

select * from bli


--JOIN

select w.country, w.y2017, w.y2018, w.y2019, b.life_satisfaction
from weo w
left join bli b
on w.country = b.country
where b.life_satisfaction is not null
order by b.life_satisfaction desc
