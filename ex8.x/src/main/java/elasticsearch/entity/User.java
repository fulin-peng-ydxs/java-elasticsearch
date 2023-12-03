package elasticsearch.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class User {

    private Integer id;
    private Integer age;
    private String name;
    private String address;
}
