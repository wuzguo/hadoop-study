package com.hadoop.study.recommend.controller;


import com.hadoop.study.recommend.entity.UserEntity;
import com.hadoop.study.recommend.service.UserService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/user")
public class UserController {

    @Autowired
    private UserService userService;

    @GetMapping(value = "/login")
    @ResponseBody
    public ModelMap login(@RequestParam("username") String username, @RequestParam("password") String password) {
        ModelMap model = new ModelMap();
        // 查询用户数据
        UserEntity userEntity = new UserEntity(username, password);
        ModelMap query = userService.login(userEntity);
        if (Boolean.parseBoolean(query.get("result").toString())) {
            model.addAttribute("success", true);
            model.addAttribute("user", query.get("user"));
        } else {
            model.addAttribute("success", false);
            model.addAttribute("msg", query.get("msg"));
        }
        return model;
    }

    @GetMapping(value = "/register")
    @ResponseBody
    public ModelMap register(@RequestParam("username") String username, @RequestParam("password") String password) {
        ModelMap model = new ModelMap();
        // 查询用户数据
        UserEntity userEntity = new UserEntity(username, password);
        UserEntity userEntity2 = userService.findByName(username);
        if (userEntity2 != null) {
            model.addAttribute("success", false);
            model.addAttribute("msg", "用户已存在");
        } else {
            model.addAttribute("success", true);
            userEntity.setTimestamp(System.currentTimeMillis());
            UserEntity res = userService.add(userEntity);
            model.addAttribute("user", res);
            model.addAttribute("msg", "注册成功");
        }
        return model;
    }
}
