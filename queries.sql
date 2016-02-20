
-- Find voter records where they were registered before they were alive.
select *, (registration_date - date_of_birth) as date_diff from voter_records where registration_date > '1900-12-31' and registration_date < date_of_birth order by date_diff;

