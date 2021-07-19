package com.hadoop.study.recommend.service;

import com.hadoop.study.recommend.dao.UserRepository;
import com.hadoop.study.recommend.entity.UserEntity;
import javax.annotation.Resource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.ui.ModelMap;

@Service
public class UserService {

    @Autowired
    private UserRepository userRepository;

    @Resource
    private BCryptPasswordEncoder bCryptPasswordEncoder;

    public UserEntity add(UserEntity userEntity) {
        userEntity.setPassword(bCryptPasswordEncoder.encode(userEntity.getPassword()));
        return userRepository.save(userEntity);
    }

    public ModelMap login(UserEntity userEntity) {
        ModelMap modelMap = new ModelMap();
        UserEntity user = userRepository.getUserByName(userEntity.getName());
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

    public UserEntity findByName(String name) {
        return userRepository.getUserByName(name);
    }
}
