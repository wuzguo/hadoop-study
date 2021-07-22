package com.hadoop.study.recommend.service;

import com.hadoop.study.recommend.dao.UserRepository;
import com.hadoop.study.recommend.entity.User;
import java.util.Calendar;
import javax.annotation.Resource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.ui.ModelMap;

@Service
public class UserService {

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Resource
    private BCryptPasswordEncoder bCryptPasswordEncoder;

    public User add(User userEntity) {
        userEntity.setPassword(bCryptPasswordEncoder.encode(userEntity.getPassword()));
        userEntity.setCreateTime(Calendar.getInstance().getTimeInMillis());
        return mongoTemplate.insert(userEntity);
    }

    public ModelMap login(User userEntity) {
        ModelMap modelMap = new ModelMap();
        User user = userRepository.getUserByName(userEntity.getName());
        if (user == null) {
            modelMap.addAttribute("result", false);
            modelMap.addAttribute("msg", "用户不存在");
            return modelMap;
        }

        if (!bCryptPasswordEncoder.matches(userEntity.getPassword(), user.getPassword())) {
            modelMap.addAttribute("result", false);
            modelMap.addAttribute("msg", "密码错误");
            return modelMap;
        }

        modelMap.addAttribute("result", true);
        modelMap.addAttribute("user", user);
        return modelMap;
    }

    public User findByName(String name) {
        return userRepository.getUserByName(name);
    }
}
