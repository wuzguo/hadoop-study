package com.hadoop.study.recommend.service;

import com.hadoop.study.recommend.entity.User;
import java.util.Calendar;
import javax.annotation.Resource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.ui.ModelMap;

@Service
public class UserService {

    @Autowired
    private MongoTemplate mongoTemplate;

    @Resource
    private BCryptPasswordEncoder bCryptPasswordEncoder;

    public User add(User user) {
        user.setPassword(bCryptPasswordEncoder.encode(user.getPassword()));
        user.setCreateTime(Calendar.getInstance().getTimeInMillis());
        return mongoTemplate.insert(user);
    }

    public ModelMap login(User user) {
        ModelMap modelMap = new ModelMap();
        User user2 = findByName(user.getName());
        if (user2 == null) {
            modelMap.addAttribute("result", false);
            modelMap.addAttribute("msg", "用户不存在");
            return modelMap;
        }

        if (!bCryptPasswordEncoder.matches(user.getPassword(), user2.getPassword())) {
            modelMap.addAttribute("result", false);
            modelMap.addAttribute("msg", "密码错误");
            return modelMap;
        }

        modelMap.addAttribute("result", true);
        modelMap.addAttribute("user", user2);
        return modelMap;
    }

    public User findByName(String name) {
        Criteria criteria = Criteria.where("name").is(name);
        return mongoTemplate.findOne(new Query(criteria), User.class);
    }
}
