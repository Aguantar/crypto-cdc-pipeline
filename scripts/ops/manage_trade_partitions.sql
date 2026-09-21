DELIMITER $$
DROP PROCEDURE IF EXISTS crypto_db.manage_trade_partitions$$
CREATE PROCEDURE crypto_db.manage_trade_partitions()
  BEGIN
    DECLARE tomorrow_n BIGINT; DECLARE nm VARCHAR(16); DECLARE cutoff_n BIGINT; DECLARE done INT DEFAULT 0; DECLARE pn VARCHAR(64); DECLARE pd BIGINT;
    DECLARE cur CURSOR FOR SELECT partition_name, CAST(partition_description AS UNSIGNED) FROM information_schema.partitions WHERE table_schema='crypto_db' AND table_name='crypto_trades' AND partition_name <> 'p_max' AND partition_description <> 'MAXVALUE';
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
    SET tomorrow_n = FLOOR(UNIX_TIMESTAMP(UTC_DATE()) / 86400) + 1;   -- 내일의 일 번호
    SET nm = CONCAT('p', DATE_FORMAT(UTC_DATE() + INTERVAL 1 DAY, '%Y%m%d'));
    IF NOT EXISTS (SELECT 1 FROM information_schema.partitions WHERE table_schema='crypto_db' AND table_name='crypto_trades' AND partition_name = nm) THEN
      SET @sql = CONCAT('ALTER TABLE crypto_db.crypto_trades REORGANIZE PARTITION p_max INTO (PARTITION ', nm, ' VALUES LESS THAN (', tomorrow_n + 1, '), PARTITION p_max VALUES LESS THAN MAXVALUE)');
      PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;
    END IF;
    -- p_max 자가 치유: 미래·미생성 날짜의 행이 p_max 에 있으면 그 날짜 파티션을 만들어 옮긴다 (알림 대신 구조로 해결)
    SET done = 0;
    WHILE (SELECT count(*) FROM crypto_db.crypto_trades PARTITION (p_max)) > 0 AND done < 10 DO
      SET @dn = (SELECT min(upbit_timestamp DIV 86400000) FROM crypto_db.crypto_trades PARTITION (p_max));
      SET @nm2 = CONCAT('p', DATE_FORMAT(FROM_UNIXTIME(@dn * 86400), '%Y%m%d'));
      SET @sql = CONCAT('ALTER TABLE crypto_db.crypto_trades REORGANIZE PARTITION p_max INTO (PARTITION ', @nm2, ' VALUES LESS THAN (', @dn + 1, '), PARTITION p_max VALUES LESS THAN MAXVALUE)');
      PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;
      SET done = done + 1;
    END WHILE;
    SET done = 0;
    SET cutoff_n = FLOOR(UNIX_TIMESTAMP(UTC_DATE()) / 86400) - 7;    -- 7일 지난 파티션(상한 ≤ cutoff) DROP
    OPEN cur;
    read_loop: LOOP
      FETCH cur INTO pn, pd; IF done = 1 THEN LEAVE read_loop; END IF;
      IF pd <= cutoff_n THEN SET @sql = CONCAT('ALTER TABLE crypto_db.crypto_trades DROP PARTITION ', pn); PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s; END IF;
    END LOOP;
    CLOSE cur;
END$$
DELIMITER ;
