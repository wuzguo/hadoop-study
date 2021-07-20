// vue.config.js
module.exports = {
    // 基本路径
    publicPath: '.',
    devServer: {
        host: '0.0.0.0',
        port: 8000,
        proxy: {
            '/api': {
                target: 'http://127.0.0.1:6800',
                changeOrigin: true,
                ws: true,
                pathRewrite: {
                    '^/api': '/'
                }
            }
        },
        disableHostCheck: true,
    }
}