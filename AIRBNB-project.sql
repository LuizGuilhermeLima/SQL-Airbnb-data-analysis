create database airbnb;
use airbnb;
drop table if exists airbnb.calendar;

create table airbnb.calendar (
    listing_id            bigint,
    dt                    char(10),
    available             char(1),
    price                  varchar(20)
);

truncate airbnb.calendar;


-- load data into the calendar table
load data local infile '/Users/luizlima/Desktop/WeCloudData/SQL/Projects/airbnb/calendar.csv'
into table airbnb.calendar
fields terminated by ',' ENCLOSED BY '"'
lines terminated by '\n'
ignore 1 lines
;

select * from calendar limit 5;
select * from calendar where listing_id=14204600 and dt='2017-08-20';

drop table if exists airbnb.listings;

create table airbnb.listings (
    id bigint,
    listing_url text,
    scrape_id bigint,
    last_scraped char(10),
    name text,
    summary text,
    space text,
    description text,
    experiences_offered text,
    neighborhood_overview text,
    notes text,
    transit text,
    access text,
    interaction text,
    house_rules text,
    thumbnail_url text,
    medium_url text,
    picture_url text,
    xl_picture_url text,
    host_id bigint,
    host_url text,
    host_name varchar(100),
    host_since char(10),
    host_location text,
    host_about text,
    host_response_time text,
    host_response_rate text,
    host_acceptance_rate text,
    host_is_superhost char(1),
    host_thumbnail_url text,
    host_picture_url text,
    host_neighbourhood text,
    host_listings_count int,
    host_total_listings_count int,
    host_verifications text,
    host_has_profile_pic char(1),
    host_identity_verified char(1),
    street text,
    neighbourhood text,
    neighbourhood_cleansed text,
    neighbourhood_group_cleansed text,
    city text,
    state text,
    zipcode text,
    market text,
    smart_location text,
    country_code text,
    country text,
    latitude text,
    longitude text,
    is_location_exact text,
    property_type text,
    room_type text,
    accommodates int,
    bathrooms text,
    bedrooms text,
    beds text,
    bed_type text,
    amenities text,
    square_feet text,
    price text,
    weekly_price text,
    monthly_price text,
    security_deposit text,
    cleaning_fee text,
    guests_included int,
    extra_people text,
    minimum_nights int,
    maximum_nights int,
    calendar_updated text,
    has_availability varchar(10),
    availability_30 int,
    availability_60 int,
    availability_90 int,
    availability_365 int,
    calendar_last_scraped varchar(10),
    number_of_reviews int,
    first_review varchar(10),
    last_review varchar(10),
    review_scores_rating text,
    review_scores_accuracy text,
    review_scores_cleanliness text,
    review_scores_checkin text,
    review_scores_communication text,
    review_scores_location text,
    review_scores_value text,
    requires_license char(1),
    license text,
    jurisdiction_names text,
    instant_bookable char(1),
    cancellation_policy varchar(20),
    require_guest_profile_picture char(1),
    require_guest_phone_verification char(1),
    calculated_host_listings_count int,
    reviews_per_month text
);

load data local infile '/Users/luizlima/Desktop/WeCloudData/SQL/Projects/airbnb/listings.csv'
into table listings
fields terminated by ',' ENCLOSED BY '"'
lines terminated by '\n'
ignore 1 lines
;

select * from listings limit 5;

drop table if exists airbnb.reviews;

create table airbnb.reviews (
    listing_id bigint,
    id bigint,
    date varchar(10),
    reviewer_id bigint,
    reviewer_name text,
    comments text
);

load data local infile '/Users/luizlima/Desktop/WeCloudData/SQL/Projects/airbnb/reviews.csv'
into table reviews
fields terminated by ',' ENCLOSED BY '"'
lines terminated by '\n'
ignore 1 lines
;

select * from reviews limit 5;

-- 1: How many unique listings is provided in the calendar table?

select count(distinct listing_id) as unique_listings
from calendar;


-- 2: How many calendar years do the calendar table span?
-- (Expected output: e.g., this table has data from 2009 to 2010)

select year(dt), month(dt)
from calendar
group by year(dt), month(dt)
order by year(dt), month(dt);

-- this table has data from 09-2016 to 09-2017


-- 3: Find listings that are completely available for the entire year (available for 365 days)


SELECT *
from(
    select listing_id, sum(days_available) as total_available
    from(
        select listing_id,
        case when available = 't' then 1
            when available = 'f' then 0 end as days_available
        from calendar) as sub1
    group by listing_id) as sub2
where total_available=365;


-- 4: How many listings have been completely booked for the year (0 days available)?

select count(distinct listing_id) as completely_booked
from(
    SELECT *
    from(
        select listing_id, sum(days_available) as total_available
        from(
            select listing_id,
            case when available = 't' then 1
                when available = 'f' then 0 end as days_available
            from calendar) as sub1
        group by listing_id) as sub2
    where total_available=0) as sub3;

-- 5: Which city has most listings?


select host_location, count(host_location) as total_listings
from listings
group by host_location
order by total_listings desc;


-- 6: Which street/st/ave has the most number of listings in Boston?
-- (Note: beacon street and beacon st should be considered the same street)

select host_location, substring_index(street,' ',1) as streets ,count(*) as total_listings
from listings
where host_location like 'Boston%'
group by host_location, streets
order by total_listings desc; 

-- 7: In the calendar table, how many listings charge different prices for weekends and weekdays?
-- Hint: use average weekend price vs average weekday price

select count(week_weekend_difference) as different_prices
from (
    select *, (avg_price-Avg_week_Price) as week_weekend_difference
    from (
        select *, lag(avg_price) over (partition by listing_id) as Avg_week_Price
        from(
            select listing_id, day, avg(prices) as avg_price
            from (
                select *, replace(price, '$', '') as prices
                from (
                    select listing_id , case when weekday(dt) in (5,6) then 'weekend'
                    when weekday(dt) in (0,1,2,3,4) then 'week' end as day, price
                    from calendar) as sub1) as sub2
            group by listing_id, day) as sub3) as sub4) as sub5
where week_weekend_difference != 0;


