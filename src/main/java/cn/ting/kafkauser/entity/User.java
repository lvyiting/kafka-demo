package cn.ting.kafkauser.entity;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author : lvyiting
 * @date : 2025-05-03
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
public class User implements Serializable {
	private String name;
	private Integer age;
}
